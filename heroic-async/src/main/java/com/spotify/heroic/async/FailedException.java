package com.spotify.heroic.async;

/**
 * Exception thrown by {@link Future#get} when a callback has failed.
 *
 * The cause of this exception will contain the reason for the failure.
 *
 * @author udoprog
 */
public class FailedException extends Exception {
    private static final long serialVersionUID = -8743981906095590L;

    public FailedException(Exception cause) {
        super(cause);
    }
}
