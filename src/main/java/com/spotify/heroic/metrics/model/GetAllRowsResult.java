package com.spotify.heroic.metrics.model;

import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.spotify.heroic.metrics.kairosdb.DataPointsRowKey;

/**
 * Results of one backend for get all rows request.
 * 
 */
@RequiredArgsConstructor
@ToString(of = { "rows" })
public class GetAllRowsResult {
    @Getter
    private final Map<String, List<DataPointsRowKey>> rows;
}