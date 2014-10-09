package com.spotify.heroic.filter;

import java.util.Comparator;

public class FilterComparator implements Comparator<Filter> {
    private FilterComparator() {
    }

    private static final FilterComparator INSTANCE = new FilterComparator();

    public static FilterComparator get() {
        return INSTANCE;
    }

    @Override
    public int compare(Filter a, Filter b) {
        if (a == null || b == null)
            throw new NullPointerException();

        if (!(a instanceof Filter) || !(b instanceof Filter))
            throw new IllegalArgumentException();

        return Integer.compare(a.hashCode(), b.hashCode());
    }
}
