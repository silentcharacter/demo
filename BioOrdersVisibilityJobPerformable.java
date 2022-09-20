/*
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */
package com.bioivt.core.job;

import com.bioivt.core.daos.EntriesQuantityDao;
import com.bioivt.core.model.BioOrdersVisibilityCronJobModel;
import com.bioivt.core.model.SageOrderModel;
import com.bioivt.core.model.SageQuoteModel;
import de.hybris.platform.core.Registry;
import de.hybris.platform.core.TenantAwareThreadFactory;
import de.hybris.platform.core.model.order.AbstractOrderModel;
import de.hybris.platform.cronjob.enums.CronJobResult;
import de.hybris.platform.cronjob.enums.CronJobStatus;
import de.hybris.platform.servicelayer.cronjob.AbstractJobPerformable;
import de.hybris.platform.servicelayer.cronjob.PerformResult;
import de.hybris.platform.servicelayer.model.ModelService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

import static com.bioivt.core.daos.impl.OrderEntriesQuantityDao.ENTRY_TYPE;


/**
 * Cronjob updates IsFullyLoaded flag on Orders and Quotes basing on comparison of SageLinesCount/SageShipmentsCount
 * and quantity of entries/shipments actually loaded into Hybris
 */
public class BioOrdersVisibilityJobPerformable extends AbstractJobPerformable<BioOrdersVisibilityCronJobModel> {
    private static final Logger LOG = LoggerFactory.getLogger(BioOrdersVisibilityJobPerformable.class);

    private final int PARALLELISM = Runtime.getRuntime().availableProcessors();

    @Resource
    private ModelService modelService;
    @Resource
    private EntriesQuantityDao orderEntriesQuantityDao;
    @Resource
    private EntriesQuantityDao quoteEntriesQuantityDao;

    @Override
    public PerformResult perform(final BioOrdersVisibilityCronJobModel cronJob) {
        final long startTime = System.currentTimeMillis();
        final Date lastExecTime = cronJob.getLastExecutionTime() == null ?  new Date(0L) : cronJob.getLastExecutionTime();
        LOG.info("Starting bioOrdersVisibilityCronJob... Last execution time [{}]", lastExecTime);

        final ExecutorService executor = Executors.newFixedThreadPool(PARALLELISM + 1,
                new TenantAwareThreadFactory(Registry.getCurrentTenant()));
        final BlockingQueue<Map<Integer, List<Long>>> queue = new LinkedBlockingDeque<>(PARALLELISM * 2);

        processObjects(orderEntriesQuantityDao, cronJob.getBatchSize(), lastExecTime, cronJob.getStartTime(),
                this::populateOrdersVisibilityFlag, "Orders", SageOrderModel.class, executor, queue);
        processObjects(quoteEntriesQuantityDao, cronJob.getBatchSize(), lastExecTime, cronJob.getStartTime(),
                this::populateQuotesVisibilityFlag, "Quotes", SageQuoteModel.class, executor, queue);

        updateLastExecTime(cronJob, cronJob.getStartTime());
        long endTime = System.currentTimeMillis();
        LOG.info("bioOrdersVisibilityCronJob is finished in " + (endTime - startTime) + "ms");
        return new PerformResult(CronJobResult.SUCCESS, CronJobStatus.FINISHED);
    }

    private void processObjects(final EntriesQuantityDao dao, final int batchSize, final Date startTime, final Date endTime,
                                Function<List<List<Object>>, List<AbstractOrderModel>> processor, final String typeName,
                                final Class _class, final ExecutorService executor, BlockingQueue<Map<Integer, List<Long>>> queue) {
        LOG.info("Starting to process {}", typeName);
        final CountDownLatch latch = new CountDownLatch(PARALLELISM);
        //in parallel start pushing data pages into the queue and invoke data consumers processing pages one by one
        executor.execute(() -> loadPages(dao, batchSize, startTime, endTime, typeName, queue));
        for (int i = 0; i < PARALLELISM; i++) {
            executor.execute(() -> processPages(dao, processor, typeName, _class, latch, queue));
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOG.error("InterruptedException", e);
        }
    }

    private void loadPages(EntriesQuantityDao dao, int batchSize, Date startTime, Date endTime, String typeName,
                           BlockingQueue<Map<Integer, List<Long>>> queue) {
        int pageNo = 0;
        try {
            try {
                while (true) {
                    LOG.info("Retrieving {} page #{}, page size {}", typeName, pageNo, batchSize);
                    var pks = dao.getPagedObjectsByEntriesCreationTime(startTime, endTime, batchSize, pageNo);
                    if (pks.isEmpty()) {
                        LOG.info("{} page #{} is empty, fetching is finished", typeName, pageNo);
                        break;
                    }
                    queue.put(Map.of(pageNo++, pks));
                }
            } finally {
                for (int i = 0; i < PARALLELISM; i++) {
                    queue.put(Collections.emptyMap());
                }
            }
        } catch (InterruptedException ex) {
            LOG.error("InterruptedException", ex);
        }
    }

    private void processPages(EntriesQuantityDao dao, Function<List<List<Object>>, List<AbstractOrderModel>> processor,
                              String typeName, Class _class, CountDownLatch latch, BlockingQueue<Map<Integer, List<Long>>> queue) {
        try {
            while(true) {
                final Map<Integer, List<Long>> page = queue.poll(2, TimeUnit.MINUTES);
                if (page == null || page.isEmpty()) {
                    break;
                }
                final Map.Entry<Integer, List<Long>> entry = page.entrySet().iterator().next();
                final int pageNo = entry.getKey();
                final List<Long> pks = entry.getValue();
                LOG.info("Processing {} page #{}", typeName, pageNo);
                modelService.saveAll(processor.apply(dao.getObjectsWithCountByPKs(pks, _class)));
                LOG.info("{} page #{} is updated", typeName, pageNo);
            }
        } catch (Exception e) {
            LOG.error("Error during page processing", e);
        }
        latch.countDown();
    }

    private List<AbstractOrderModel> populateQuotesVisibilityFlag(final List<List<Object>> rows) {
        final List<AbstractOrderModel> result = new ArrayList<>();
        for (List<Object> row : rows) {
            final SageQuoteModel quote = (SageQuoteModel) row.get(0);
            final Long linesCount = (Long) row.get(1);
            quote.setIsFullyLoaded(quote.getSageLinesCount() <= linesCount);
            result.add(quote);
        }
        return result;
    }

    private List<AbstractOrderModel> populateOrdersVisibilityFlag(final List<List<Object>> rows) {
        //group rows <OrderModel, Count, Type> by orders
        final Map<SageOrderModel, Stats> map = new HashMap<>();
        rows.forEach(row -> {
            final SageOrderModel order = (SageOrderModel) row.get(0);
            map.putIfAbsent(order, map.getOrDefault(order, new Stats()).accept(row));
        });
        final List<AbstractOrderModel> result = new ArrayList<>();
        map.forEach((order, stats) -> {
            order.setIsFullyLoaded(
                    order.getSageLinesCount() <= stats.entries && order.getSageShipmentsCount() <= stats.consignments);
            result.add(order);
        });
        return result;
    }

    private void updateLastExecTime(final BioOrdersVisibilityCronJobModel cronJob, final Date lastExecTime) {
        LOG.info("Setting LastExecutionTime: {}", lastExecTime);
        cronJob.setLastExecutionTime(lastExecTime);
        modelService.save(cronJob);
    }

    private static class Stats {
        long entries = 0;
        long consignments = 0;

        public Stats accept(final List<Object> row) {
            if (ENTRY_TYPE.equals(row.get(2))) {
                entries = (long) row.get(1);
            } else {
                consignments = (long) row.get(1);
            }
            return this;
        }
    }
}
