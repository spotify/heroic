package com.spotify.heroic.aggregator;

import java.util.List;

import com.spotify.heroic.backend.kairosdb.DataPoint;

public class AggregatorGroup {
    private final List<Aggregator> aggregators;

    public AggregatorGroup(List<Aggregator> aggregators) {
        this.aggregators = aggregators;
    }

    public long getIntervalHint() {
        long max = 0;

        for (Aggregator aggregator : aggregators) {
            final long hint = aggregator.getIntervalHint();

            if (hint > max) {
                max = hint;
            }
        }

        return max;
    }

    public boolean isEmpty() {
        return aggregators.isEmpty();
    }

    public List<DataPoint> aggregate(List<DataPoint> datapoints) {
        for (final Aggregator aggregator : aggregators) {
            datapoints = aggregator.aggregate(datapoints);
        }

        return datapoints;
    }
}
