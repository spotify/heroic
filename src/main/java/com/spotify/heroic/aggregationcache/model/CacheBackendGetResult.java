package com.spotify.heroic.aggregationcache.model;

import java.util.List;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.spotify.heroic.model.DataPoint;

@ToString(of = { "key", "datapoints" })
@RequiredArgsConstructor
public class CacheBackendGetResult {
    @Getter
    private final CacheBackendKey key;
    @Getter
    private final List<DataPoint> datapoints;
}
