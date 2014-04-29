package com.spotify.heroic.cache.model;

import lombok.Getter;
import lombok.ToString;

import com.spotify.heroic.model.CacheKey;
import com.spotify.heroic.model.DataPoint;

@ToString(of = { "cacheKey", "datapoints" })
public class CacheBackendGetResult {
    @Getter
    private final CacheKey cacheKey;
    @Getter
    private final DataPoint[] datapoints;

    public CacheBackendGetResult(CacheKey cacheKey, DataPoint[] datapoints) {
        this.cacheKey = cacheKey;
        this.datapoints = datapoints;
    }
}
