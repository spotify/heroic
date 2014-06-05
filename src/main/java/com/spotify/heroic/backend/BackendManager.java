package com.spotify.heroic.backend;

import java.util.List;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.backend.model.GroupedAllRowsResult;
import com.spotify.heroic.http.model.MetricsQuery;
import com.spotify.heroic.http.model.MetricsQueryResult;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.TimeSerie;

public interface BackendManager {
    @ToString(of={"timeSerie", "datapoints"})
    @RequiredArgsConstructor
    public static final class MetricGroup {
        @Getter
        private final TimeSerie timeSerie;

        @Getter
        private final List<DataPoint> datapoints;
    }

    @ToString(of={"groups", "statistics"})
    @RequiredArgsConstructor
    public static final class MetricGroups {
        @Getter
        private final List<MetricGroup> groups;
        @Getter
        private final Statistics statistics;
    }

    public static interface MetricStream {
        public void stream(Callback<StreamMetricsResult> callback, MetricsQueryResult result)
                throws Exception;
    }

    public Callback<MetricsQueryResult> queryMetrics(MetricsQuery query)
            throws QueryException;

    @ToString(of={})
    @RequiredArgsConstructor
    public static final class StreamMetricsResult {
    }

    public Callback<StreamMetricsResult> streamMetrics(MetricsQuery query, MetricStream handle)
            throws QueryException;

    public Callback<GroupedAllRowsResult> getAllRows();
}
