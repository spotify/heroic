package com.spotify.heroic.filter.impl;

import lombok.Data;
import lombok.EqualsAndHashCode;

import com.spotify.heroic.filter.Filter;

@Data
@EqualsAndHashCode(of = { "OPERATOR", "tag", "value" }, doNotUseGetters = true)
public class StartsWithFilterImpl implements Filter.StartsWith {
    public static final String OPERATOR = "^";

    private final String tag;
    private final String value;

    @Override
    public String toString() {
        return "[" + OPERATOR + ", " + tag + ", " + value + "]";
    }

    @Override
    public StartsWithFilterImpl optimize() {
        return this;
    }

    @Override
    public String operator() {
        return OPERATOR;
    }

    @Override
    public String first() {
        return tag;
    }

    @Override
    public String second() {
        return value;
    }

    @Override
    public int compareTo(Filter o) {
        if (!Filter.StartsWith.class.isAssignableFrom(o.getClass()))
            return operator().compareTo(o.operator());

        final Filter.StartsWith other = (Filter.StartsWith) o;
        final int first = FilterComparatorUtils.stringCompare(first(), other.first());

        if (first != 0)
            return first;

        return FilterComparatorUtils.stringCompare(second(), other.second());
    }
}