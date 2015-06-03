package com.spotify.heroic.filter;


public interface MultiArgumentsFilter<T extends Filter.MultiArgs<A>, A> extends MultiArgumentsFilterBuilder<T, A>,
        FilterJsonSerialization<T> {
}