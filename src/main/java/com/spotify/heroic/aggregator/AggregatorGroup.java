package com.spotify.heroic.aggregator;

import java.util.ArrayList;
import java.util.List;

import com.spotify.heroic.aggregator.Aggregator.Result;
import com.spotify.heroic.model.DataPoint;

public class AggregatorGroup {
    private static final class Session implements Aggregator.Session {
        private final Aggregator.Session first;
        private final Iterable<Aggregator.Session> rest;
        private final Aggregation aggregation;

        public Session(Aggregator.Session first,
                Iterable<Aggregator.Session> rest, Aggregation aggregation) {
            this.first = first;
            this.rest = rest;
            this.aggregation = aggregation;
        }

        @Override
        public void stream(Iterable<DataPoint> datapoints) {
            first.stream(datapoints);
        }

        @Override
        public Result result() {
            final Result partial = first.result();
            List<DataPoint> datapoints = partial.getResult();
            final long sampleSize = partial.getSampleSize();
            final long outOfBounds = partial.getOutOfBounds();

            for (final Aggregator.Session session : rest) {
                session.stream(datapoints);
                final Result next = session.result();
                datapoints = next.getResult();
            }

            return new Result(datapoints, sampleSize, outOfBounds);
        }

        @Override
        public Aggregation getAggregation() {
            return aggregation;
        }
    }

    private final List<Aggregator> aggregators;
    private final Aggregation aggregation;

    public AggregatorGroup(List<Aggregator> aggregators, Aggregation aggregation) {
        this.aggregators = aggregators;
        this.aggregation = aggregation;
    }

    public long getIntervalHint() {
        long max = 0;

        for (final Aggregator aggregator : aggregators) {
            final long hint = aggregator.getIntervalHint();

            if (hint > max) {
                max = hint;
            }
        }

        return max;
    }

    public Aggregator.Session session() {
        if (aggregators.isEmpty()) {
            return null;
        }

        final Aggregator.Session first = aggregators.get(0).session();
        final List<Aggregator.Session> rest = new ArrayList<Aggregator.Session>();

        for (final Aggregator aggregator : aggregators.subList(1,
                aggregators.size())) {
            rest.add(aggregator.session());
        }

        return new Session(first, rest, aggregation);
    }

    /**
     * Get a guesstimate of how big of a memory all aggregations would need.
     * This is for the invoker to make the decision whether or not to execute
     * the aggregation.
     * 
     * @return
     */
    public long getCalculationMemoryMagnitude() {
        long sum = 0;
        for (final Aggregator aggregator : aggregators) {
            sum += aggregator.getCalculationMemoryMagnitude();
        }
        return sum;
    }
}
