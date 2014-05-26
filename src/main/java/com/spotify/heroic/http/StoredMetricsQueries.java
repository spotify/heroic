package com.spotify.heroic.http;

import java.util.concurrent.ConcurrentHashMap;

import javax.inject.Singleton;

import com.spotify.heroic.query.MetricsQuery;

public class StoredMetricsQueries {
    private final ConcurrentHashMap<String, MetricsQuery> storedQueries = new ConcurrentHashMap<String, MetricsQuery>();

    public void put(String id, MetricsQuery query) {
        storedQueries.put(id, query);
    }

    public MetricsQuery get(String id) {
        return storedQueries.get(id);
    }
}
