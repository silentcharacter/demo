package com.bioivt.core.daos.impl;

import com.bioivt.core.daos.EntriesQuantityDao;

public class QuoteEntriesQuantityDao extends EntriesQuantityDao {

    private static final String QUOTES_PAGE_QUERY =
            "SELECT DISTINCT {order} FROM {SageQuoteEntry} " +
                    "WHERE {creationtime} > ?start and {creationtime} <= ?end ORDER BY {order}";

    private static final String QUOTES_GROUPED_QUERY =
            "SELECT {e.order}, count(*), '' as type  FROM {SageQuoteEntry as e} " +
                    "WHERE {e.order} in (?PKs) " +
                    "GROUP BY {e.order}";

    @Override
    protected String getByEntriesCreationTimeQuery() {
        return QUOTES_PAGE_QUERY;
    }

    @Override
    protected String getByPkQuery() {
        return QUOTES_GROUPED_QUERY;
    }
}
