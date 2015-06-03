package com.spotify.heroic.filter;

public interface OneArgumentFilter<T extends Filter.OneArg<O>, O> extends OneArgumentFilterBuilder<T, O>,
        FilterJsonSerialization<T> {
}