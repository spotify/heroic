package com.spotify.heroic.filter;

import java.util.List;

public interface FilterFactory {
    public Filter.And and(Filter... filters);

    public Filter.And and(List<Filter> filters);

    public Filter.Or or(Filter... filters);

    public Filter.Or or(List<Filter> filters);

    public Filter.Not not(Filter filter);

    public Filter.True t();

    public Filter.False f();

    public Filter.MatchKey matchKey(String value);

    public Filter.MatchTag matchTag(String key, String value);

    public Filter.HasTag hasTag(String tag);

    public Filter.StartsWith startsWith(String tag, String value);

    public Filter.Regex regex(String tag, String value);
}
