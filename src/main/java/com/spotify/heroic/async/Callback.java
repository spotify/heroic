package com.spotify.heroic.async;

/**
 * Interface for asynchronous callbacks with the ability to subscribe to
 * interesting events.
 * 
 * @author udoprog
 * 
 * @param <T>
 *            The type being realized in the callback's finish method.
 */
public interface Callback<T> {
    public static enum State {
        INITIALIZED, FAILED, FINISHED, CANCELLED
    }

    public static interface Cancelled {
        void cancel() throws Exception;
    }

    public static interface Ended {
        void ended() throws Exception;
    }

    public static interface Handle<T> extends Cancelled {
        void error(Throwable e) throws Exception;

        void finish(T result) throws Exception;
    }

    public Callback<T> fail(Throwable error);

    public Callback<T> finish(T result);

    public Callback<T> cancel();

    public Callback<T> register(Handle<T> handle);

    public Callback<T> register(Ended ended);

    public Callback<T> register(Cancelled cancelled);

    public boolean isInitialized();
}
