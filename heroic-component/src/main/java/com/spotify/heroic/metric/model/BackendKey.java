package com.spotify.heroic.metric.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import lombok.Data;

import com.spotify.heroic.model.Series;

import eu.toolchain.async.Collector;

@Data
public class BackendKey {
    private final Series series;
    private final long base;

    private static Collector<List<BackendKey>, List<BackendKey>> collector = new Collector<List<BackendKey>, List<BackendKey>>() {
        @Override
        public List<BackendKey> collect(Collection<List<BackendKey>> results) throws Exception {
            final List<BackendKey> result = new ArrayList<>();

            for (final List<BackendKey> r : results)
                result.addAll(r);

            return result;
        }
    };

    public static Collector<List<BackendKey>, List<BackendKey>> merge() {
        return collector;
    }
}