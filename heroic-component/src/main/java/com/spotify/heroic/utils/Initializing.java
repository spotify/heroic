package com.spotify.heroic.utils;

/**
 * The given implementation needs to do some initialization before it is ready to receive requests.
 *
 * This gives access to {@code #isReady()} which allows users to check if it is ready or not.
 *
 * @author udoprog
 */
public interface Initializing {
    public boolean isReady();
}