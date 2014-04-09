package com.spotify.heroic.async;

import lombok.Getter;
import lombok.ToString;

@ToString(of = { "message" })
public class CancelReason {

    @Getter
    private final String message;

    public CancelReason(String message) {
        this.message = message;
    }
}
