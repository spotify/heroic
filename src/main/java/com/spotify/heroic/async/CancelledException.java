package com.spotify.heroic.async;

import lombok.Getter;

/**
 * Exception thrown by {@link Callback#get} when a callback has been cancelled.
 *
 * @author udoprog
 */
public class CancelledException extends Exception {
    private static final long serialVersionUID = 6484865638951685720L;

    @Getter
    private final CancelReason reason;

    public CancelledException(CancelReason reason) {
        super(reason.getMessage());
        this.reason = reason;
    }
}
