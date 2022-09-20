package com.bioivt.core.daos;

import de.hybris.platform.servicelayer.search.FlexibleSearchQuery;
import de.hybris.platform.servicelayer.search.FlexibleSearchService;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

public abstract class EntriesQuantityDao {

    @Resource
    private FlexibleSearchService flexibleSearchService;

    protected abstract String getByEntriesCreationTimeQuery();

    protected abstract String getByPkQuery();

    public List<Long> getPagedObjectsByEntriesCreationTime(final Date startTime, final Date endTime, int pageSize, int pageNo) {
        var query = new FlexibleSearchQuery(getByEntriesCreationTimeQuery(), Map.of("start", startTime, "end", endTime));
        query.setCount(pageSize);
        query.setStart(pageSize * pageNo);
        return flexibleSearchService.<Long>search(query).getResult();
    }

    public List<List<Object>> getObjectsWithCountByPKs(final List<Long> PKs, final Class _class) {
        var query = new FlexibleSearchQuery(getByPkQuery(), Map.of("PKs", PKs));
        query.setResultClassList(Arrays.asList(_class, Long.class, String.class));
        return flexibleSearchService.<List<Object>>search(query).getResult();
    }
}
