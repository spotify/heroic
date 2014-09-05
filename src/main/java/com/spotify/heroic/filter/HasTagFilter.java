package com.spotify.heroic.filter;

import lombok.Data;

@Data
public class HasTagFilter implements OneTermFilter, Comparable<Filter> {
    public static final String OPERATOR = "+";
    private final String tag;

    @Override
    public String toString() {
        return "[" + OPERATOR + ", " + tag + "]";
    }

    @Override
    public HasTagFilter optimize() {
        return this;
    }

    @Override
    public String operator() {
        return OPERATOR;
    }

    @Override
    public int compareTo(Filter o) {
        if (o == null)
            return -1;

        if (!(o instanceof Filter))
            return -1;

        if (!(o instanceof HasTagFilter))
            return operator().compareTo(o.operator());

        return Integer.compare(hashCode(), o.hashCode());
    }

    @Override
    public String first() {
        return tag;
    }
}
