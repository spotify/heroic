package com.spotify.heroic.cache.model;

import java.util.List;

import lombok.Getter;
import lombok.ToString;

import com.spotify.heroic.model.DataPoint;

@ToString(of = { "key", "datapoints" })
public class CacheBackendGetResult {
    @Getter
    private final CacheBackendKey key;
    @Getter
    private final List<DataPoint> datapoints;

    public CacheBackendGetResult(CacheBackendKey key, List<DataPoint> datapoints) {
        this.key = key;
        this.datapoints = datapoints;
    }
}
