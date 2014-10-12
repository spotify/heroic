package com.spotify.heroic.async;

import java.util.Collection;

public interface ErrorReducer<C, T> {
    T reduce(Collection<C> results, Collection<CancelReason> cancelled, Collection<Exception> errors) throws Exception;
}
