package com.spotify.heroic.backend.model;

import java.util.Set;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.spotify.heroic.model.TimeSerie;

/**
 * Grouped results of all backends for get all rows request.
 */
@RequiredArgsConstructor
@ToString(of = { "timeSeries" })
public class GroupedAllRowsResult {
    @Getter
    private final Set<TimeSerie> timeSeries;
}