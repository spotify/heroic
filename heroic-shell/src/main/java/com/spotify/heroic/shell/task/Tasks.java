package com.spotify.heroic.shell.task;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;

import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.RangeFilter;

public final class Tasks {
    public static Filter setupFilter(FilterFactory filters, QueryParser parser, QueryParams params) {
        final List<String> query = params.getQuery();

        if (query.isEmpty())
            return filters.t();

        return parser.parseFilter(StringUtils.join(query, " "));
    }

    public abstract static class QueryParamsBase implements QueryParams {
        private final DateRange defaultDateRange;

        public QueryParamsBase() {
            final long now = System.currentTimeMillis();
            final long start = now - TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS);
            this.defaultDateRange = new DateRange(start, now);
        }

        @Override
        public DateRange getRange() {
            return defaultDateRange;
        }
    }

    public static interface QueryParams {
        public List<String> getQuery();

        public DateRange getRange();

        public int getLimit();
    }

    public static RangeFilter setupRangeFilter(FilterFactory filters, QueryParser parser, QueryParams params) {
        final Filter filter = setupFilter(filters, parser, params);
        return new RangeFilter(filter, params.getRange(), params.getLimit());
    }
}
