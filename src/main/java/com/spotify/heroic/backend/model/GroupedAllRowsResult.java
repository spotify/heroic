package com.spotify.heroic.backend.model;

import java.util.Set;

import lombok.Getter;
import lombok.ToString;

import com.spotify.heroic.backend.TimeSerie;

/**
 * Grouped results of all backends for get all rows request.
 */
@ToString(of = { "timeSeries" })
public class GroupedAllRowsResult {
    @Getter
    private final Set<TimeSerie> timeSeries;

    public GroupedAllRowsResult(Set<TimeSerie> timeSeries) {
        this.timeSeries = timeSeries;
    }
}