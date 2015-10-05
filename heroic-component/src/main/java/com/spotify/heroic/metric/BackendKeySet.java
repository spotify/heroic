package com.spotify.heroic.metric;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;

import eu.toolchain.async.Collector;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class BackendKeySet implements Iterable<BackendKey> {
    private final List<BackendKey> keys;
    private final Optional<QueryTrace> trace;

    public BackendKeySet() {
        this(ImmutableList.of(), Optional.empty());
    }

    public BackendKeySet(List<BackendKey> keys) {
        this(keys, Optional.empty());
    }

    public int size() {
        return keys.size();
    }

    public boolean isEmpty() {
        return keys.isEmpty();
    }

    @Override
    public Iterator<BackendKey> iterator() {
        return keys.iterator();
    }

    public static Collector<BackendKeySet, BackendKeySet> collect(final QueryTrace.Identifier what) {
        final Stopwatch w = Stopwatch.createStarted();

        return results -> {
            final List<BackendKey> result = new ArrayList<>();
            final List<QueryTrace> children = new ArrayList<>();

            for (final BackendKeySet r : results) {
                result.addAll(r.getKeys());
                r.trace.ifPresent(children::add);
            }

            final Optional<QueryTrace> trace;

            if (!children.isEmpty()) {
                trace = Optional.of(new QueryTrace(what, w.elapsed(TimeUnit.NANOSECONDS), ImmutableList.copyOf(children)));
            } else {
                trace = Optional.empty();
            }

            return new BackendKeySet(result, trace);
        };
    }
}