package com.spotify.heroic.aggregator;

import java.util.Date;
import java.util.List;

import com.spotify.heroic.backend.kairosdb.DataPoint;

public interface Aggregator {
    public static interface JSON {
        public Aggregator build(Date start, Date end);
    }

    /**
     * Calculate result of all datapoints for this aggregator.
     * 
     * Must not be thread safe.
     * 
     * @param datapoints
     * @return
     */
    List<DataPoint> aggregate(Iterable<DataPoint> datapoints);

    /**
     * Indicate weither this aggregator can be updated on-the-fly or not.
     * 
     * @return
     */
    public boolean isStreamable();

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
    public List<DataPoint> result();

    /**
     * Get a hint of how large the interval is that this aggregator will
     * require.
     * 
     * @return 0 if no interval is used, otherwise a positive value indicating
     *         the millisecond interval.
     */
    public long getIntervalHint();
}
