package com.spotify.heroic.filter;

import lombok.Data;

@Data
public class MatchKeyFilter implements OneTermFilter, Comparable<Filter> {
    public static final String OPERATOR = "key";

    private final String value;

    @Override
    public String toString() {
        return "[" + OPERATOR + ", " + value + "]";
    }

    @Override
    public MatchKeyFilter optimize() {
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

        if (!(o instanceof MatchKeyFilter))
            return operator().compareTo(o.operator());

        return Integer.compare(hashCode(), o.hashCode());
    }

    @Override
    public String first() {
        return value;
    }
}
