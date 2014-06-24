package com.spotify.heroic.injection;

/**
 * Mark the type as a delegate.
 * 
 * This is to allow for dependency injection to find the 'correct' instance to
 * wire up.
 * 
 * @author udoprog
 * @param <T>
 *            The type of the delegate.
 */
public interface Delegator<T> {
    T delegate();
}
