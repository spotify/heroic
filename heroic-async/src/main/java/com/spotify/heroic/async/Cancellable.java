package com.spotify.heroic.async;

public interface Cancellable {
    void cancelled(CancelReason reason) throws Exception;
}