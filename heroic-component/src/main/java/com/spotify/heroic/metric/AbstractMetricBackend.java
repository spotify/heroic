package com.spotify.heroic.metric;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.collect.ImmutableList;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Transform;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class AbstractMetricBackend implements MetricBackend {
    private final AsyncFramework async;

    @Override
    public AsyncFuture<Iterator<BackendKey>> allKeys(final BackendKey start, final int limit) {
        return keys(start, limit).transform(new Transform<List<BackendKey>, Iterator<BackendKey>>() {
            @Override
            public Iterator<BackendKey> transform(final List<BackendKey> initialResult) throws Exception {
                if (initialResult.isEmpty()) {
                    return Collections.emptyIterator();
                }

                return new Iterator<BackendKey>() {
                    /* future for the next batch */
                    AsyncFuture<List<BackendKey>> future = keys(initialResult.get(initialResult.size() - 1), limit);
                    /* iterator over the current batch */
                    Iterator<BackendKey> iterator = initialResult.iterator();

                    boolean done = false;

                    @Override
                    public boolean hasNext() {
                        if (done) {
                            return false;
                        }

                        if (!iterator.hasNext()) {
                            final List<BackendKey> nextBatch;

                            try {
                                nextBatch = future.get();
                            } catch (Exception e) {
                                throw new RuntimeException("Failed to get next batch", e);
                            }

                            /* null to help out GC */
                            iterator = null;
                            future = null;

                            if (nextBatch.isEmpty()) {
                                done = true;
                                return false;
                            }

                            future = keys(nextBatch.get(nextBatch.size() - 1), limit);
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

    @Override
    public AsyncFuture<List<String>> serializeKeyToHex(BackendKey key) {
        return async.resolved(ImmutableList.of());
    }

    @Override
    public AsyncFuture<List<BackendKey>> deserializeKeyFromHex(String key) {
        return async.resolved(ImmutableList.of());
    }
}