package com.spotify.heroic.filter;

public interface NoArgumentFilterBuilder<T extends Filter.NoArg> {
    public T build();
}