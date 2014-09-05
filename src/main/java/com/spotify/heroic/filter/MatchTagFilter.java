package com.spotify.heroic.filter;

import lombok.Data;

@Data
public class MatchTagFilter implements TwoTermsFilter, Comparable<Filter> {
    public static final String OPERATOR = "=";

    private final String tag;
    private final String value;

    @Override
    public String toString() {
        return "[" + OPERATOR + ", " + tag + ", " + value + "]";
    }

    @Override
    public MatchTagFilter optimize() {
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

        if (!(o instanceof MatchTagFilter))
            return operator().compareTo(o.operator());

        return Integer.compare(hashCode(), o.hashCode());
    }

    @Override
    public String first() {
        return tag;
    }

    @Override
    public String second() {
        return value;
    }
}
