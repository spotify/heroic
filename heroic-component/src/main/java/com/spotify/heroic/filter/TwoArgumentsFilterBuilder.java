package com.spotify.heroic.filter;

public interface TwoArgumentsFilterBuilder<T extends Filter.TwoArgs<A, B>, A, B> {
    public T build(A first, B second);
}