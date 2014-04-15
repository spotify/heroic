package com.spotify.heroic.backend.model;

import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.ToString;

import com.spotify.heroic.backend.kairosdb.DataPointsRowKey;

/**
 * Results of one backend for get all rows request.
 * 
 */
@ToString(of = { "rows" })
public class GetAllRowsResult {
    @Getter
    private final Map<String, List<DataPointsRowKey>> rows;

    public GetAllRowsResult(Map<String, List<DataPointsRowKey>> rows) {
        this.rows = rows;
    }
}