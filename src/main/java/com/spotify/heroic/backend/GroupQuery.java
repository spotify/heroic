package com.spotify.heroic.backend;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GroupQuery<T> {
    public static interface Handle<T> {
        void done(Collection<T> results, Collection<Throwable> errors,
                int cancelled) throws Exception;
    }

    private final AtomicInteger countdown;
    private final List<Query<T>> queries;
    private final List<Handle<T>> handlers = new LinkedList<Handle<T>>();
    private volatile boolean done = false;

    private final Queue<Throwable> errors = new ConcurrentLinkedQueue<Throwable>();
    private final Queue<T> results = new ConcurrentLinkedQueue<T>();
    private final AtomicInteger cancelled = new AtomicInteger();

    private final Query.Handle<T> listener = new Query.Handle<T>() {
        @Override
        public void error(Throwable e) throws Exception {
            errors.add(e);
            GroupQuery.this.check();
        }

        @Override
        public void finish(T result) throws Exception {
            results.add(result);
            GroupQuery.this.check();
        }

        @Override
        public void cancel() throws Exception {
            cancelled.incrementAndGet();
            GroupQuery.this.check();
        }
    };

    public GroupQuery(List<Query<T>> queries) {
        this.countdown = new AtomicInteger(queries.size());
        this.queries = queries;
        this.done = false;

        for (Query<T> query : queries)
            query.listen(listener);

        if (queries.isEmpty())
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
        for (Query<T> query : queries) {
            query.cancel();
        }
    }
}
