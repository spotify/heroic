package com.spotify.heroic.aggregator;

import java.util.List;

import com.spotify.heroic.aggregator.Aggregator.Result;
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

    public void stream(Iterable<DataPoint> datapoints) {
        if (aggregators.isEmpty()) {
            throw new RuntimeException("Aggregator group is empty");
        }

        final Aggregator first = aggregators.get(0);
        first.stream(datapoints);
    }

    public Result result() {
        if (aggregators.isEmpty()) {
            throw new RuntimeException("Aggregator group is empty");
        }

        final Aggregator first = aggregators.get(0);
        final List<Aggregator> rest = aggregators
                .subList(1, aggregators.size());

        Result partial = first.result();
        List<DataPoint> datapoints = partial.getResult();
        long sampleSize = partial.getSampleSize();
        long outOfBounds = partial.getOutOfBounds();

        for (final Aggregator aggregator : rest) {
            aggregator.stream(datapoints);
            final Result next = aggregator.result();
            datapoints = next.getResult();
            sampleSize += next.getSampleSize();
            outOfBounds += next.getOutOfBounds();
        }

        return new Result(datapoints, sampleSize, outOfBounds);
    }
}
