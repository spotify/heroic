package com.spotify.heroic.aggregation;

import java.util.List;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Sampling;
import com.spotify.heroic.model.Statistics;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
public interface Aggregation {
    @Data
    public static class Result {
        private final List<DataPoint> result;
        private final Statistics.Aggregator statistics;
    }

    public interface Session {
        /**
         * Stream datapoints into this aggregator.
         *
         * Must be thread-safe.
         *
         * @param datapoints
         */
        public void update(Iterable<DataPoint> datapoints);

        /**
         * Get the result of this aggregator.
         */
        public Result result();
    }

    /**
     * Create an aggregation session.
     *
     * @return
     */
    public Session session(DateRange range);

    /**
     * Get a hint of the sampling this aggregation uses.
     */
    public Sampling getSampling();
}