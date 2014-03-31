package com.spotify.heroic.query;

import java.util.Date;
import java.util.List;

import com.spotify.heroic.backend.kairosdb.DataPoint;

public interface Aggregator {
    List<DataPoint> aggregate(Date start, Date end, List<DataPoint> datapoints);
}
