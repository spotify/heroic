package com.spotify.heroic.async;

import lombok.Getter;
import lombok.ToString;

@ToString(of = { "message" })
public class CancelReason {
    public static final CancelReason BACKEND_DISABLED = new CancelReason(
            "Backend disabled");

    public static final CancelReason NO_BACKENDS_AVAILABLE = new CancelReason(
            "No backends available");

    @Getter
    private final String message;

    public CancelReason(String message) {
        this.message = message;
    }
}
