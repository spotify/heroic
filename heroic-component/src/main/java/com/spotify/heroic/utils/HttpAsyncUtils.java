package com.spotify.heroic.utils;

import javax.ws.rs.container.AsyncResponse;

import eu.toolchain.async.AsyncFuture;

public interface HttpAsyncUtils {
    public interface Resume<T, R> {
        public R resume(T value) throws Exception;
    }

    public <T> void handleAsyncResume(final AsyncResponse response, final AsyncFuture<T> callback);

    /**
     * Helper function to correctly wire up async response management.
     *
     * @param response The async response object.
     * @param callback Callback for the pending request.
     * @param resume The resume implementation.
     */
    public <T, R> void handleAsyncResume(final AsyncResponse response, final AsyncFuture<T> callback,
            final Resume<T, R> resume);

    public <T> Resume<T, T> passthrough();
}
