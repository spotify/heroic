package com.spotify.heroic.async;

import java.util.Collection;

/**
 * Simplified abstraction on top of CallbackGroup meant to reduce the result of multiple queries into one.
 *
 * Will be called when the entire result is available. If this is undesirable, use {@link #StreamReducer}.
 *
 * @author udoprog
 */
public interface Reducer<C, R> {
    R resolved(Collection<C> results, Collection<Exception> errors, Collection<CancelReason> cancelled)
            throws Exception;
}