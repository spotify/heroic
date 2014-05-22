package com.spotify.heroic.aggregator;

import java.util.List;

import lombok.Getter;

import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;

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
    public long getWidth();

    /**
     * Get a guesstimate of how big of a memory the aggregation would need. This
     * is for the invoker to make the decision whether or not to execute the
     * aggregation.
     * 
     * @return
     */
    public long getCalculationMemoryMagnitude(DateRange range);
}
