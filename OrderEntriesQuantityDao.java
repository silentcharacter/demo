package com.bioivt.core.daos.impl;

import com.bioivt.core.daos.EntriesQuantityDao;

public class OrderEntriesQuantityDao extends EntriesQuantityDao {

    public static final String ENTRY_TYPE = "entry";
    private static final String CONS_TYPE = "cons";

    private static final String ORDERS_PAGE_QUERY =
            "SELECT DISTINCT x.o FROM ({{" +
                    "   SELECT {order} as o FROM {SageOrderEntry} " +
                    "   WHERE {creationtime} > ?start and {creationtime} <= ?end " +
                    "}} UNION ALL {{" +
                    "   SELECT {order} as o FROM {Consignment} " +
                    "   WHERE {creationtime} > ?start and {creationtime} <= ?end " +
                    "}}) x ORDER BY x.o";

    private static final String ORDERS_GROUPED_QUERY =
            "SELECT x.o, x.num, x.type FROM ({{" +
                    "   SELECT {e.order} as o, count(*) as num, '" + ENTRY_TYPE + "' as type FROM {SageOrderEntry as e} " +
                    "   WHERE {e.order} in (?PKs) " +
                    "   GROUP BY {e.order} " +
                    "}} UNION ALL {{" +
                    "   SELECT {c.order} as o, count(*) as num, '" + CONS_TYPE + "' as type FROM {Consignment as c} " +
                    "   WHERE {c.order} in (?PKs) " +
                    "   GROUP BY {c.order} " +
                    "}}) x";

    @Override
    protected String getByEntriesCreationTimeQuery() {
        return ORDERS_PAGE_QUERY;
    }

    @Override
    protected String getByPkQuery() {
        return ORDERS_GROUPED_QUERY;
    }
}