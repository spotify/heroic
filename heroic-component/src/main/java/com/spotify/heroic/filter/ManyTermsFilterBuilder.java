package com.spotify.heroic.filter;

import java.util.Collection;

public interface ManyTermsFilterBuilder<T extends ManyTermsFilter> {
    T build(Collection<Filter> filters);
}