package com.spotify.heroic.metrics.heroic;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.backend.QueryException;
import com.spotify.heroic.metrics.MetricBackend;
import com.spotify.heroic.metrics.model.FetchDataPoints;
import com.spotify.heroic.metrics.model.FindTimeSeries;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;

public class HeroicMetricBackend implements MetricBackend {
    @Override
    public List<Callback<FetchDataPoints.Result>> query(
            final TimeSerie timeSerie, final DateRange range) {
        return new ArrayList<Callback<FetchDataPoints.Result>>();
    }

    @Override
    public Callback<FindTimeSeries.Result> findTimeSeries(FindTimeSeries query)
            throws QueryException {
        return null;
    }

    @Override
    public Callback<Set<TimeSerie>> getAllTimeSeries() {
        return null;
    }

    @Override
    public Callback<Long> getColumnCount(TimeSerie timeSerie, DateRange range) {
        return null;
    }
}
