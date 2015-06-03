package com.spotify.heroic.filter;

import java.util.Collection;

public interface MultiArgumentsFilterBuilder<T extends Filter.MultiArgs<A>, A> {
    public T build(Collection<A> terms);
}