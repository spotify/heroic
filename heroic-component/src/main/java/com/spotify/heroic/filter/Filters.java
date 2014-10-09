package com.spotify.heroic.filter;

import java.util.Arrays;

public final class Filters {
    public static final TrueFilter TRUE = TrueFilter.get();
    public static final FalseFilter FALSE = FalseFilter.get();

    public static AndFilter and(Filter... filters) {
        return new AndFilter(Arrays.asList(filters));
    }

    public static OrFilter or(Filter... filters) {
        return new OrFilter(Arrays.asList(filters));
    }

    public static NotFilter not(Filter filter) {
        return new NotFilter(filter);
    }

    public static TrueFilter t() {
        return TrueFilter.get();
    }

    public static FalseFilter f() {
        return FalseFilter.get();
    }
}
