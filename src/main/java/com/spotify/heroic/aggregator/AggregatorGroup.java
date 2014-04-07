package com.spotify.heroic.aggregator;

import java.util.ArrayList;
import java.util.List;

import com.spotify.heroic.aggregator.Aggregator.Result;
import com.spotify.heroic.backend.kairosdb.DataPoint;

public class AggregatorGroup {
    private final List<Aggregator> aggregators;

    private static final class Session implements
            Aggregator.Session {
        private final Aggregator.Session first;
        private final Iterable<Aggregator.Session> rest;

        public Session(Aggregator.Session first,
                Iterable<Aggregator.Session> rest) {
            this.first = first;
            this.rest = rest;
        }

        @Override
        public void stream(Iterable<DataPoint> datapoints) {
            first.stream(datapoints);
        }

        @Override
        public Result result() {
            Result partial = first.result();
            List<DataPoint> datapoints = partial.getResult();
            long sampleSize = partial.getSampleSize();
            long outOfBounds = partial.getOutOfBounds();

            for (final Aggregator.Session session : rest) {
                session.stream(datapoints);
                final Result next = session.result();
                datapoints = next.getResult();
            }

            return new Result(datapoints, sampleSize, outOfBounds);
        }
    }

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

    public Aggregator.Session session() {
        if (aggregators.isEmpty()) {
            return null;
        }
        
        final Aggregator.Session first = aggregators.get(0).session();
        final List<Aggregator.Session> rest = new ArrayList<Aggregator.Session>();
        
        for (Aggregator aggregator : aggregators.subList(1, aggregators.size())) {
            rest.add(aggregator.session());
        }
        
        return new Session(first, rest);
    }
}
