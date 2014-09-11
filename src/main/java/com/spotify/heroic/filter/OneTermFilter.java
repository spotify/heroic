package com.spotify.heroic.filter;

public interface OneTermFilter<T> extends Filter {
    public T first();
}
