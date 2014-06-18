package com.spotify.heroic.aggregator;

import java.util.ArrayList;
import java.util.List;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.aggregator.Aggregator.Result;
import com.spotify.heroic.metrics.model.Statistics;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;

@ToString(of = { "aggregators" })
@EqualsAndHashCode(of = { "aggregators" })
public class AggregatorGroup {
    private static final class Session implements Aggregator.Session {
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
            final Result firstResult = first.result();
            List<DataPoint> datapoints = firstResult.getResult();
            Statistics.Aggregator statistics = firstResult.getStatistics();

            for (final Aggregator.Session session : rest) {
                session.stream(datapoints);
                final Result next = session.result();
                datapoints = next.getResult();
                statistics = statistics.merge(next.getStatistics());
            }

            return new Result(datapoints, statistics);
        }
    }

    @Getter
    private final AggregationGroup aggregationGroup;

    private final List<Aggregator> aggregators;

    @Getter
    private final long width;

    public AggregatorGroup(AggregationGroup aggregationGroup, List<Aggregator> aggregators) {
        this.aggregationGroup = aggregationGroup;
        this.aggregators = aggregators;
        this.width = calculateWidth(aggregators);
    }

    private long calculateWidth(List<Aggregator> aggregators) {
        long max = 0;

        for (final Aggregator aggregator : aggregators) {
            final long hint = aggregator.getWidth();

            if (hint > max) {
                max = hint;
            }
        }

        return max;
    }

    public Aggregator.Session session(DateRange range) {
        if (aggregators.isEmpty()) {
            return null;
        }

        final Aggregator.Session first = aggregators.get(0).session(range);
        final List<Aggregator.Session> rest = new ArrayList<Aggregator.Session>();

        for (final Aggregator aggregator : aggregators.subList(1,
                aggregators.size())) {
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
        for (final Aggregator aggregator : aggregators) {
            sum += aggregator.getCalculationMemoryMagnitude(range);
        }
        return sum;
    }
}
