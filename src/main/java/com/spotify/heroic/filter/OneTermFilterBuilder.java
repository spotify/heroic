package com.spotify.heroic.filter;

public interface OneTermFilterBuilder<T> {
    T build(String first);
}