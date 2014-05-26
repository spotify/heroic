package com.spotify.heroic.backend;

import java.util.List;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.backend.model.GroupedAllRowsResult;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.query.MetricsQuery;

public interface BackendManager {
    @ToString(of={"timeSerie", "datapoints"})
    @RequiredArgsConstructor
    public static final class DataPointGroup {
        @Getter
        private final TimeSerie timeSerie;

        @Getter
        private final List<DataPoint> datapoints;
    }

    @ToString(of={"groups", "statistics"})
    @RequiredArgsConstructor
    public static final class QueryMetricsResult {
        @Getter
        private final List<DataPointGroup> groups;
        @Getter
        private final Statistics statistics;
    }

    public Callback<QueryMetricsResult> queryMetrics(MetricsQuery query)
            throws QueryException;

    public Callback<GroupedAllRowsResult> getAllRows();
}
