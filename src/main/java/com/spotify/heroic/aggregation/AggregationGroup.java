package com.spotify.heroic.aggregation;

import java.util.ArrayList;
import java.util.List;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import com.spotify.heroic.metrics.model.Statistics;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;

@ToString(of = { "aggregations" })
@EqualsAndHashCode(of = { "aggregations" })
public class AggregationGroup {
    private static final class Session implements Aggregation.Session {
        private final Aggregation.Session first;
        private final Iterable<Aggregation.Session> rest;

        public Session(Aggregation.Session first,
                Iterable<Aggregation.Session> rest) {
            this.first = first;
            this.rest = rest;
        }

        @Override
        public void stream(Iterable<DataPoint> datapoints) {
            first.stream(datapoints);
        }

        @Override
        public Aggregation.Result result() {
            final Aggregation.Result firstResult = first.result();
            List<DataPoint> datapoints = firstResult.getResult();
            Statistics.Aggregator statistics = firstResult.getStatistics();

            for (final Aggregation.Session session : rest) {
                session.stream(datapoints);
                final Aggregation.Result next = session.result();
                datapoints = next.getResult();
                statistics = statistics.merge(next.getStatistics());
            }

            return new Aggregation.Result(datapoints, statistics);
        }
    }

    @Getter
    private final List<Aggregation> aggregations;

    @Getter
    private final long width;

    public AggregationGroup(List<Aggregation> aggregations) {
        this.aggregations = aggregations;
        this.width = calculateWidth(aggregations);
    }

    private long calculateWidth(List<Aggregation> aggregations) {
        if (aggregations.isEmpty())
            return 0;

        final Aggregation last = aggregations.get(aggregations.size() - 1);
        return last.getSampling().getSize();
    }

    public Aggregation.Session session(DateRange range) {
        if (aggregations.isEmpty()) {
            return null;
        }

        final Aggregation.Session first = aggregations.get(0).session(range);
        final List<Aggregation.Session> rest = new ArrayList<Aggregation.Session>();

        for (final Aggregation aggregator : aggregations.subList(1,
                aggregations.size())) {
            rest.add(aggregator.session(range));
        }

        return new Session(first, rest);
    }

    /**
     * Get a guesstimate of how big of a memory all aggregations would need.
     * This is for the invoker to make the decision whether or not to execute
     * the aggregation.
     * 
     * @return
     */
    public long getCalculationMemoryMagnitude(DateRange range) {
        long sum = 0;

        for (final Aggregation aggregator : aggregations) {
            sum += aggregator.getCalculationMemoryMagnitude(range);
        }

        return sum;
    }
}