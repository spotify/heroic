package com.spotify.heroic.filter;

public interface NoArgumentFilter<T extends Filter.NoArg> extends NoArgumentFilterBuilder<T>,
        FilterJsonSerialization<T> {
}