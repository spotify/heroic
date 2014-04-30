package com.spotify.heroic.aggregator;

import java.util.List;

import lombok.Getter;

import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.query.DateRange;

public interface Aggregator {
    public static class Result {
        @Getter
        private final List<DataPoint> result;
        @Getter
        private final long sampleSize;
        @Getter
        private final long outOfBounds;

        public Result(List<DataPoint> result, long sampleSize, long outOfBounds) {
            this.result = result;
            this.sampleSize = sampleSize;
            this.outOfBounds = outOfBounds;
        }
    }

    public interface Session {
        /**
         * Stream datapoints into this aggregator.
         * 
         * Must be thread-safe.
         * 
         * @param datapoints
         */
        public void stream(Iterable<DataPoint> datapoints);

        /**
         * Get the result of this aggregator.
         */
        public Result result();

        /**
         * Get the aggregation type this session belongs to.
         * 
         * @return The aggregation type this session belongs to.
         */
        public Aggregation getAggregation();
    }

    /**
     * Create an aggregation session.
     * 
     * @return
     */
    public Session session(DateRange range);

    /**
     * Get a hint of how large the interval is that this aggregator will
     * require.
     * 
     * @return 0 if no interval is used, otherwise a positive value indicating
     *         the millisecond interval.
     */
    public long getIntervalHint();

    /**
     * Get a guesstimate of how big of a memory the aggregation would need. This
     * is for the invoker to make the decision whether or not to execute the
     * aggregation.
     * 
     * @return
     */
    public long getCalculationMemoryMagnitude(DateRange range);
}
