package com.spotify.heroic.filter;

public interface TwoArgumentsFilter<T extends Filter.TwoArgs<A, B>, A, B> extends TwoArgumentsFilterBuilder<T, A, B>,
        FilterJsonSerialization<T> {
}