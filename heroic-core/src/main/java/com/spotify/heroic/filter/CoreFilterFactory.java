package com.spotify.heroic.filter;

import java.util.Arrays;
import java.util.List;

import com.spotify.heroic.filter.Filter.MatchKey;
import com.spotify.heroic.filter.Filter.Regex;
import com.spotify.heroic.filter.impl.AndFilterImpl;
import com.spotify.heroic.filter.impl.FalseFilterImpl;
import com.spotify.heroic.filter.impl.HasTagFilterImpl;
import com.spotify.heroic.filter.impl.MatchKeyFilterImpl;
import com.spotify.heroic.filter.impl.MatchTagFilterImpl;
import com.spotify.heroic.filter.impl.NotFilterImpl;
import com.spotify.heroic.filter.impl.OrFilterImpl;
import com.spotify.heroic.filter.impl.RegexFilterImpl;
import com.spotify.heroic.filter.impl.StartsWithFilterImpl;
import com.spotify.heroic.filter.impl.TrueFilterImpl;

public final class CoreFilterFactory implements FilterFactory {
    @Override
    public Filter.And and(Filter... filters) {
        return new AndFilterImpl(Arrays.asList(filters));
    }

    @Override
    public Filter.And and(List<Filter> filters) {
        return new AndFilterImpl(filters);
    }

    @Override
    public Filter.Or or(Filter... filters) {
        return new OrFilterImpl(Arrays.asList(filters));
    }

    @Override
    public Filter.Or or(List<Filter> filters) {
        return new OrFilterImpl(filters);
    }

    @Override
    public Filter.Not not(Filter filter) {
        return new NotFilterImpl(filter);
    }

    @Override
    public Filter.True t() {
        return TrueFilterImpl.get();
    }

    @Override
    public Filter.False f() {
        return FalseFilterImpl.get();
    }

    @Override
    public Filter.MatchTag matchTag(String key, String value) {
        return new MatchTagFilterImpl(key, value);
    }

    @Override
    public Filter.HasTag hasTag(String tag) {
        return new HasTagFilterImpl(tag);
    }

    @Override
    public Filter.StartsWith startsWith(String tag, String value) {
        return new StartsWithFilterImpl(tag, value);
    }

    @Override
    public MatchKey matchKey(String value) {
        return new MatchKeyFilterImpl(value);
    }

    @Override
    public Regex regex(String tag, String value) {
        return new RegexFilterImpl(tag, value);
    }
}