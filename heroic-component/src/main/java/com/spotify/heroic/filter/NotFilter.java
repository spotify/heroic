package com.spotify.heroic.filter;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(of = { "OPERATOR", "filter" }, doNotUseGetters = true)
public class NotFilter implements OneTermFilter<Filter> {
    public static final String OPERATOR = "not";

    public static final OneTermFilterBuilder<NotFilter, Filter> BUILDER = new OneTermFilterBuilder<NotFilter, Filter>() {
        @Override
        public NotFilter build(Filter filter) {
            return new NotFilter(filter);
        }
    };

    private final Filter filter;

    @Override
    public String toString() {
        return "[" + OPERATOR + ", " + filter + "]";
    }

    @Override
    public Filter optimize() {
        if (filter instanceof NotFilter) {
            return ((NotFilter) filter).getFilter();
        }

        return this;
    }

    @Override
    public String operator() {
        return OPERATOR;
    }

    @Override
    public Filter first() {
        return filter;
    }
}
