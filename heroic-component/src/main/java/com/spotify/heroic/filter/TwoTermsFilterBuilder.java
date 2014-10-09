package com.spotify.heroic.filter;

public interface TwoTermsFilterBuilder<T extends TwoTermsFilter> {
    T build(String first, String second);
}