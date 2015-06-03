package com.spotify.heroic.filter.impl;

import lombok.Data;
import lombok.EqualsAndHashCode;

import com.spotify.heroic.filter.Filter;

@Data
@EqualsAndHashCode(of = { "OPERATOR", "tag" }, doNotUseGetters = true)
public class HasTagFilterImpl implements Filter.HasTag {
    public static final String OPERATOR = "+";

    private final String tag;

    @Override
    public String toString() {
        return "[" + OPERATOR + ", " + tag + "]";
    }

    @Override
    public HasTagFilterImpl optimize() {
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

    public static Filter of(String tag) {
        return new HasTagFilterImpl(tag);
    }

    @Override
    public int compareTo(Filter o) {
        if (!Filter.HasTag.class.isAssignableFrom(o.getClass()))
            return operator().compareTo(o.operator());

        final Filter.HasTag other = (Filter.HasTag) o;
        return FilterComparatorUtils.stringCompare(this.tag, other.first());
    }
}