package com.spotify.heroic.metric;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Transform;

public class MetricBackends {
    private static final Consumer<BackendKeySet> doNothing = (set) -> {};

    public static AsyncFuture<Iterator<BackendKey>> allKeys(final BackendKey start, final int limit, final BiFunction<BackendKey, Integer, AsyncFuture<BackendKeySet>> fetcher) {
        return keysPager(start, limit, fetcher, doNothing);
    }

    public static AsyncFuture<Iterator<BackendKey>> keysPager(final BackendKey start, final int limit, final BiFunction<BackendKey, Integer, AsyncFuture<BackendKeySet>> fetcher, final Consumer<BackendKeySet> setConsumer) {
        return fetcher.apply(start, limit).directTransform(new Transform<BackendKeySet, Iterator<BackendKey>>() {
            @Override
            public Iterator<BackendKey> transform(final BackendKeySet initialResult) throws Exception {
                setConsumer.accept(initialResult);

                if (initialResult.isEmpty()) {
                    return Collections.emptyIterator();
                }

                return new Iterator<BackendKey>() {
                    /* future for the next batch */
                    AsyncFuture<BackendKeySet> future = fetcher.apply(initialResult.getKeys().get(initialResult.size() - 1), limit);
                    /* iterator over the current batch */
                    Iterator<BackendKey> iterator = initialResult.iterator();

                    boolean done = false;

                    @Override
                    public boolean hasNext() {
                        if (done) {
                            return false;
                        }

                        if (!iterator.hasNext()) {
                            final BackendKeySet nextBatch;

                            try {
                                nextBatch = future.get();
                            } catch (Exception e) {
                                throw new RuntimeException("Failed to get next batch", e);
                            }

                            /* null to help out GC */
                            iterator = null;
                            future = null;

                            setConsumer.accept(nextBatch);

                            if (nextBatch.isEmpty()) {
                                done = true;
                                return false;
                            }

                            future = fetcher.apply(nextBatch.getKeys().get(nextBatch.size() - 1), limit);
                            iterator = nextBatch.iterator();
                        }

                        return true;
                    }

                    @Override
                    public BackendKey next() {
                        if (done) {
                            throw new NoSuchElementException();
                        }

                        return iterator.next();
                    }
                };
            }
        });
    };
}