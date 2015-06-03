package com.spotify.heroic.filter.impl;

import lombok.Data;
import lombok.EqualsAndHashCode;

import com.spotify.heroic.filter.Filter;

@Data
@EqualsAndHashCode(of = { "OPERATOR" }, doNotUseGetters = true)
public class TrueFilterImpl implements Filter.True {
    public static final String OPERATOR = "true";

    private static final Filter.True instance = new TrueFilterImpl();

    public static Filter.True get() {
        return instance;
    }

    @Override
    public String toString() {
        return "[" + OPERATOR + "]";
    }

    @Override
    public TrueFilterImpl optimize() {
        return this;
    }

    @Override
    public String operator() {
        return OPERATOR;
    }

    @Override
    public Filter invert() {
        return FalseFilterImpl.get();
    }

    @Override
    public int compareTo(Filter o) {
        if (!Filter.True.class.isAssignableFrom(o.getClass()))
            return operator().compareTo(o.operator());

        return 0;
    }
}