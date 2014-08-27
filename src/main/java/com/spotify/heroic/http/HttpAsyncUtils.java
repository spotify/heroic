package com.spotify.heroic.http;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.CompletionCallback;
import javax.ws.rs.container.ConnectionCallback;
import javax.ws.rs.container.TimeoutHandler;
import javax.ws.rs.core.Response;

import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.http.general.MessageResponse;

@Slf4j
public final class HttpAsyncUtils {
    public interface Resume<T, R> {
        public R resume(T value) throws Exception;
    }

    /**
     * Helper function to correctly wire up async response management.
     *
     * @param response
     *            The async response object.
     * @param callback
     *            Callback for the pending request.
     * @param resume
     *            The resume implementation.
     */
    public static <T, R> void handleAsyncResume(final AsyncResponse response,
            final Callback<T> callback, final Resume<T, R> resume) {
        callback.register(new Callback.Handle<T>() {
            @Override
            public void cancelled(CancelReason reason) throws Exception {
                response.resume(Response
                        .status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity(new MessageResponse("Request cancelled: "
                                + reason)).build());
            }

            @Override
            public void failed(Exception e) throws Exception {
                response.resume(Response
                        .status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity(e).build());
            }

            @Override
            public void resolved(T result) throws Exception {
                response.resume(Response.status(Response.Status.OK)
                        .entity(resume.resume(result)).build());
            }
        });

        HttpAsyncUtils.setupAsyncHandling(response, callback);
    }

    private static void setupAsyncHandling(final AsyncResponse response,
            final Callback<?> callback) {
        response.setTimeoutHandler(new TimeoutHandler() {
            @Override
            public void handleTimeout(AsyncResponse asyncResponse) {
                log.info("Request timed out");
                callback.cancel(new CancelReason("Request timed out"));
            }
        });

        response.register(new CompletionCallback() {
            @Override
            public void onComplete(Throwable throwable) {
                log.info("Client completed");
                callback.cancel(new CancelReason("Client completed"));
            }
        });

        response.register(new ConnectionCallback() {
            @Override
            public void onDisconnect(AsyncResponse disconnected) {
                log.info("Client disconnected");
                callback.cancel(new CancelReason("Client disconnected"));
            }
        });
    }
}
