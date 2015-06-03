package com.spotify.heroic.filter.impl;

import lombok.Data;
import lombok.EqualsAndHashCode;

import com.spotify.heroic.filter.Filter;

@Data
@EqualsAndHashCode(of = { "OPERATOR", "filter" }, doNotUseGetters = true)
public class RawFilterImpl implements Filter.Raw {
    public static final String OPERATOR = "q";

    private final String filter;

    @Override
    public String toString() {
        return "[" + OPERATOR + ", " + filter + "]";
    }

    @Override
    public RawFilterImpl optimize() {
        return this;
    }

    @Override
    public String operator() {
        return OPERATOR;
    }

    @Override
    public String first() {
        return filter;
    }

    @Override
    public int compareTo(Filter o) {
        if (!Filter.Raw.class.isAssignableFrom(o.getClass()))
            return operator().compareTo(o.operator());

        final Filter.Raw other = (Filter.Raw) o;
        return filter.compareTo(other.first());
    }
}