package com.spotify.heroic.filter.impl;

import lombok.Data;
import lombok.EqualsAndHashCode;

import com.spotify.heroic.filter.Filter;

@Data
@EqualsAndHashCode(of = { "OPERATOR", "value" }, doNotUseGetters = true)
public class MatchKeyFilterImpl implements Filter.MatchKey {
    public static final String OPERATOR = "key";

    private final String value;

    @Override
    public String toString() {
        return "[" + OPERATOR + ", " + value + "]";
    }

    @Override
    public MatchKeyFilterImpl optimize() {
        return this;
    }

    @Override
    public String operator() {
        return OPERATOR;
    }

    @Override
    public String first() {
        return value;
    }

    @Override
    public int compareTo(Filter o) {
        if (!Filter.MatchKey.class.isAssignableFrom(o.getClass()))
            return operator().compareTo(o.operator());

        final Filter.MatchKey other = (Filter.MatchKey) o;
        return FilterComparatorUtils.stringCompare(first(), other.first());
    }
}
