package com.spotify.heroic.filter;

public interface OneTermFilterBuilder<T, O> {
    T build(O first);
}