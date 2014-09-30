package com.spotify.heroic.aggregationcache.model;

import java.util.List;

import lombok.Data;
import lombok.Getter;

import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;

@Data
public class CacheQueryResult {
    private final CacheBackendKey key;

    private final DateRange range;

    /**
     * Collected results so far. Should be joined by the result from the above
     * cache misses.
     */
    private final List<DataPoint> result;

    /**
     * Cache misses that has to be queried and aggregated from raw storage.
     */
    @Getter
    private final List<DateRange> misses;
}
