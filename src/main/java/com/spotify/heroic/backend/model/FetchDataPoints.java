package com.spotify.heroic.backend.model;

import java.util.List;

import lombok.Getter;
import lombok.ToString;

import com.spotify.heroic.backend.kairosdb.DataPointsRowKey;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;

@ToString(of = { "rows", "range" })
public class FetchDataPoints {
    @Getter
    private final List<DataPointsRowKey> rows;

    @Getter
    private final DateRange range;

    public FetchDataPoints(List<DataPointsRowKey> rows, DateRange range) {
        super();
        this.rows = rows;
        this.range = range;
    }

    @ToString(of = { "datapoints", "rowKey" })
    public static class Result {
        @Getter
        private final List<DataPoint> datapoints;

        @Getter
        private final DataPointsRowKey rowKey;

        public Result(List<DataPoint> datapoints, DataPointsRowKey rowKey) {
            this.datapoints = datapoints;
            this.rowKey = rowKey;
        }
    }
}