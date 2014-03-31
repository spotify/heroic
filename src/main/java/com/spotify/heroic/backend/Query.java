package com.spotify.heroic.backend;

import java.util.LinkedList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Query<T> {
    public static enum State {
        INITIALIZED, FAILED, FINISHED, CANCELLED
    }

    public static interface Handle<T> {
        void error(Throwable e) throws Exception;

        void finish(T result) throws Exception;

        void cancel() throws Exception;
    }

    private final List<Query.Handle<T>> handlers = new LinkedList<Query.Handle<T>>();

    private State state = Query.State.INITIALIZED;
    private Throwable error;
    private T result;

    public synchronized void fail(Throwable error) {
        if (state != Query.State.INITIALIZED)
            return;

        this.state = Query.State.FAILED;
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
        if (state != Query.State.INITIALIZED)
            return;

        this.state = Query.State.FINISHED;
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
        if (state != Query.State.INITIALIZED)
            return;

        this.state = Query.State.CANCELLED;

        for (Handle<T> handle : handlers) {
            try {
                handle.cancel();
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
                log.error("Failed to invoke finish callback", e);
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
    }
}
