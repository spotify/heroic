package com.spotify.heroic.backend;

import java.util.LinkedList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Callback<T> {
    public static enum State {
        INITIALIZED, FAILED, FINISHED, CANCELLED
    }

    public static interface Cancelled {
        void cancel() throws Exception;
    }

    public static interface Handle<T> extends Cancelled {
        void error(Throwable e) throws Exception;

        void finish(T result) throws Exception;
    }

    private final List<Callback.Handle<T>> handlers = new LinkedList<Callback.Handle<T>>();
    private final List<Callback.Cancelled> cancelled = new LinkedList<Callback.Cancelled>();

    private State state = Callback.State.INITIALIZED;
    private Throwable error;
    private T result;

    public synchronized void fail(Throwable error) {
        if (state != Callback.State.INITIALIZED)
            return;

        this.state = Callback.State.FAILED;
        this.error = error;

        for (Handle<T> handle : handlers) {
            try {
                handle.error(error);
            } catch (Exception e) {
                log.error("Failed to invoke error callback", e);
            }
        }
    }

    public synchronized void finish(T result) {
        if (state != Callback.State.INITIALIZED)
            return;

        this.state = Callback.State.FINISHED;
        this.result = result;

        for (Handle<T> handle : handlers) {
            try {
                handle.finish(result);
            } catch (Exception e) {
                log.error("Failed to invoke cancel callback", e);
            }
        }
    }

    public synchronized void cancel() {
        if (state != Callback.State.INITIALIZED)
            return;

        this.state = Callback.State.CANCELLED;

        for (Cancelled cancel : cancelled) {
            try {
                cancel.cancel();
            } catch (Exception e) {
                log.error("Failed to invoke cancel callback", e);
            }
        }
    }

    public synchronized void listen(Handle<T> handle) {
        switch (state) {
        case FINISHED:
            try {
                handle.finish(result);
            } catch (Exception e) {
                log.error("Failed to invoke finish callback", e);
            }
            return;
        case CANCELLED:
            try {
                handle.cancel();
            } catch (Exception e) {
                log.error("Failed to invoke cancel callback", e);
            }
            return;
        case FAILED:
            try {
                handle.error(error);
            } catch (Exception e2) {
                log.error("Failed to invoke error callback", e2);
            }
            return;
        default:
            break;
        }

        handlers.add(handle);
        cancelled.add(handle);
    }

    public synchronized void cancelled(Cancelled handle) {
        switch (state) {
        case CANCELLED:
            try {
                handle.cancel();
            } catch (Exception e) {
                log.error("Failed to invoke cancel callback", e);
            }
            return;
        default:
            break;
        }

        cancelled.add(handle);
    }
}
