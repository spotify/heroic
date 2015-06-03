package com.spotify.heroic.filter.impl;

import lombok.Data;
import lombok.EqualsAndHashCode;

import com.spotify.heroic.filter.Filter;

@Data
@EqualsAndHashCode(of = { "OPERATOR", "filter" }, doNotUseGetters = true)
public class NotFilterImpl implements Filter.Not {
    public static final String OPERATOR = "not";

    private final Filter filter;

    @Override
    public String toString() {
        return "[" + OPERATOR + ", " + filter + "]";
    }

    @Override
    public Filter optimize() {
        if (filter instanceof Filter.Not)
            return ((Filter.Not) filter).first().optimize();

        return new NotFilterImpl(filter.optimize());
    }

    @Override
    public String operator() {
        return OPERATOR;
    }

    @Override
    public Filter first() {
        return filter;
    }

    @Override
    public int compareTo(Filter o) {
        if (!Filter.Not.class.isAssignableFrom(o.getClass()))
            return operator().compareTo(o.operator());

        return filter.compareTo(o);
    }
}