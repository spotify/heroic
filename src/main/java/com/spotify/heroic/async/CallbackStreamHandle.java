package com.spotify.heroic.async;


/**
 * A helper class that will act as a CallbackGroup.Handle reporting it's result
 * to a Callback.Handle.
 * 
 * @author udoprog
 * 
 * @param <T>
 * @param <R>
 */
public abstract class CallbackStreamHandle<T, R> implements
        CallbackStream.Handle<R> {
    private final Callback<T> callback;

    public CallbackStreamHandle(Callback<T> callback) {
        this.callback = callback;
    }

    @Override
    public void done(int successful, int failed, int cancelled) {
        if (!callback.isInitialized())
            return;

        final T result;

        try {
            result = execute(successful, failed, cancelled);
        } catch (final Throwable t) {
            callback.fail(t);
            return;
        }

        callback.finish(result);
    }

    public abstract T execute(int successful, int failed, int cancelled)
            throws Exception;
}
