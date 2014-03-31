package com.spotify.heroic.query;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import lombok.Getter;
import lombok.Setter;

import com.spotify.heroic.backend.kairosdb.DataPoint;

public class SumAggregator implements Aggregator {
    @Getter
    @Setter
    private Resolution resolution;

    @Override
    public List<DataPoint> aggregate(Date start, Date end,
            List<DataPoint> datapoints) {
        final List<DataPoint> result = new ArrayList<DataPoint>();
        return result;
    }
}
