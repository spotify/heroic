package com.spotify.heroic.async;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CallbackGroup<T> {
    public static interface Handle<T> {
        void done(Collection<T> results, Collection<Throwable> errors,
                int cancelled) throws Exception;
    }

    private final AtomicInteger countdown;
    private final List<Callback<T>> callbacks;
    private final List<Handle<T>> handlers = new LinkedList<Handle<T>>();
    private volatile boolean done = false;

    private final Queue<Throwable> errors = new ConcurrentLinkedQueue<Throwable>();
    private final Queue<T> results = new ConcurrentLinkedQueue<T>();
    private final AtomicInteger cancelled = new AtomicInteger();

    private final Callback.Handle<T> listener = new Callback.Handle<T>() {
        @Override
        public void error(Throwable e) throws Exception {
            errors.add(e);
            CallbackGroup.this.check();
        }

        @Override
        public void finish(T result) throws Exception {
            results.add(result);
            CallbackGroup.this.check();
        }

        @Override
        public void cancel() throws Exception {
            cancelled.incrementAndGet();
            CallbackGroup.this.check();
        }
    };

    public CallbackGroup(Collection<Callback<T>> callbacks) {
        this.countdown = new AtomicInteger(callbacks.size());
        this.callbacks = new ArrayList<Callback<T>>(callbacks);
        this.done = false;

        for (Callback<T> callback : callbacks)
            callback.register(listener);

        if (callbacks.isEmpty())
            end();
    }

    private void check() {
        int value = countdown.decrementAndGet();

        log.info("{} errors:{} results:{} cancelled:{}", value, errors.size(),
                results.size(), cancelled.get());

        if (value != 0)
            return;

        end();
    }

    private synchronized void end() {
        if (done)
            return;

        for (Handle<T> handle : handlers) {
            trigger(handle);
        }

        done = true;
        handlers.clear();
    }

    private void trigger(Handle<T> handle) {
        try {
            handle.done(results, errors, cancelled.get());
        } catch (Exception e) {
            log.error("Failed to call handler", e);
        }
    }

    public synchronized void listen(Handle<T> handle) {
        if (done) {
            trigger(handle);
            return;
        }

        handlers.add(handle);
    }

    /* cancel all queries in this group */
    public void cancel() {
        for (Callback<T> callback : callbacks) {
            callback.cancel();
        }
    }
}
