package com.spotify.heroic.metric;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.ImmutableList;

import eu.toolchain.async.Collector;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class BackendKeySet implements Iterable<BackendKey> {
    private final List<BackendKey> keys;
    private final List<QueryTrace> traces;

    public BackendKeySet() {
        this(ImmutableList.of(), ImmutableList.of());
    }

    public BackendKeySet(List<BackendKey> keys) {
        this(keys, ImmutableList.of());
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

    private static Collector<BackendKeySet, BackendKeySet> collector = new Collector<BackendKeySet, BackendKeySet>() {
        @Override
        public BackendKeySet collect(Collection<BackendKeySet> results) throws Exception {
            final List<BackendKey> result = new ArrayList<>();
            final List<QueryTrace> traces = new ArrayList<>();

            for (final BackendKeySet r : results) {
                result.addAll(r.getKeys());
                traces.addAll(r.getTraces());
            }

            return new BackendKeySet(result, traces);
        }
    };

    public static Collector<BackendKeySet, BackendKeySet> merge() {
        return collector;
    }
}