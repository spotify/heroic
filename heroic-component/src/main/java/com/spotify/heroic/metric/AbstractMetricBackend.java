package com.spotify.heroic.metric;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Transform;

public abstract class AbstractMetricBackend implements MetricBackend {
    @Override
    public AsyncFuture<Iterator<BackendKey>> allKeys(final BackendKey start, final int limit) {
        return keys(start, null, limit).transform(new Transform<List<BackendKey>, Iterator<BackendKey>>() {
            @Override
            public Iterator<BackendKey> transform(final List<BackendKey> initialResult) throws Exception {
                if (initialResult.isEmpty()) {
                    return Collections.emptyIterator();
                }

                final BackendKey start = initialResult.iterator().next();

                return new Iterator<BackendKey>() {
                    Iterator<BackendKey> currentIterator = initialResult.iterator();
                    BackendKey nextStart = null;

                    @Override
                    public boolean hasNext() {
                        if (!currentIterator.hasNext()) {
                            currentIterator = getNextIterator();
                        }

                        final BackendKey next = currentIterator.next();

                        if (nextStart != null) {
                            /* we wrapped around, and are done! */
                            if (next.equals(start)) {
                                return false;
                            }
                        }

                        nextStart = next;
                        return true;
                    }

                    private Iterator<BackendKey> getNextIterator() {
                        if (nextStart == null) {
                            throw new IllegalStateException("no starting position recorded");
                        }

                        final Iterator<BackendKey> nextIterator;

                        try {
                            nextIterator = keys(nextStart, null, limit).get().iterator();
                        } catch (final Exception e) {
                            throw new RuntimeException(e);
                        }

                        if (!nextIterator.hasNext()) {
                            throw new IllegalStateException("Next iterator fetched was empty, this should never happen");
                        }

                        return nextIterator;
                    }

                    @Override
                    public BackendKey next() {
                        return nextStart;
                    }
                };
            }
        });
    };
}