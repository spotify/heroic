package com.spotify.heroic.query;

import java.util.Date;
import java.util.List;

import com.spotify.heroic.backend.kairosdb.DataPoint;

public interface Aggregator {
    List<DataPoint> aggregate(Date start, Date end, List<DataPoint> datapoints);

    /**
     * Get a hint of how large the interval is that this aggregator will
     * require.
     * 
     * @return 0 if no interval is used, otherwise a positive value indicating
     *         the millisecond interval.
     */
    public long getIntervalHint();
}
