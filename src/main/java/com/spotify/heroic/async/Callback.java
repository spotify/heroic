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

    public void fail(Throwable error);

    public void finish(T result);

    public void cancel();

    public void register(Handle<T> handle);

    public void register(Ended ended);

    public void register(Cancelled cancelled);
}
