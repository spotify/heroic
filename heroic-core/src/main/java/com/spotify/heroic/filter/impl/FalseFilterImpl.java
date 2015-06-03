package com.spotify.heroic.filter.impl;

import lombok.Data;
import lombok.EqualsAndHashCode;

import com.spotify.heroic.filter.Filter;

@Data
@EqualsAndHashCode(of = { "OPERATOR" }, doNotUseGetters = true)
public class FalseFilterImpl implements Filter.False {
    public static final String OPERATOR = "false";

    private static final Filter.False instance = new FalseFilterImpl();

    public static Filter.False get() {
        return instance;
    }

    @Override
    public String toString() {
        return "[" + OPERATOR + "]";
    }

    @Override
    public FalseFilterImpl optimize() {
        return this;
    }

    @Override
    public String operator() {
        return OPERATOR;
    }

    @Override
    public Filter invert() {
        return TrueFilterImpl.get();
    }

    @Override
    public int compareTo(Filter o) {
        if (!Filter.False.class.isAssignableFrom(o.getClass()))
            return operator().compareTo(o.operator());

        return 0;
    }
}