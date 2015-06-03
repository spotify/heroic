package com.spotify.heroic.filter.impl;

import lombok.Data;
import lombok.EqualsAndHashCode;

import com.spotify.heroic.filter.Filter;

@Data
@EqualsAndHashCode(of = { "OPERATOR", "tag", "value" }, doNotUseGetters = true)
public class MatchTagFilterImpl implements Filter.MatchTag {
    public static final String OPERATOR = "=";

    private final String tag;
    private final String value;

    @Override
    public String toString() {
        return "[" + OPERATOR + ", " + tag + ", " + value + "]";
    }

    @Override
    public MatchTagFilterImpl optimize() {
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
        if (!Filter.MatchTag.class.isAssignableFrom(o.getClass()))
            return operator().compareTo(o.operator());

        final Filter.MatchTag other = (Filter.MatchTag) o;
        final int first = FilterComparatorUtils.stringCompare(first(), other.first());

        if (first != 0)
            return first;

        return FilterComparatorUtils.stringCompare(second(), other.second());
    }
}