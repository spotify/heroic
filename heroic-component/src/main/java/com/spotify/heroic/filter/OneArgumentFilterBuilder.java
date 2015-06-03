package com.spotify.heroic.filter;

public interface OneArgumentFilterBuilder<T extends Filter.OneArg<O>, O> {
    public T build(O first);
}