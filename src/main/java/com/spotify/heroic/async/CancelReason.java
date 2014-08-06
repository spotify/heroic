package com.spotify.heroic.async;

import lombok.Data;

@Data
public class CancelReason {
    /**
     * TODO: move these to the module in which they are used!
     */
    public static final CancelReason BACKEND_DISABLED = new CancelReason(
            "Backend disabled");

    public static final CancelReason BACKEND_MISMATCH = new CancelReason(
            "Backend does not match");

    public static final CancelReason NO_BACKENDS_AVAILABLE = new CancelReason(
            "No backends available");

    public static final CancelReason NOT_SUPPORTED = new CancelReason(
            "Operation not supported");

    private final String message;
}
