package com.spotify.heroic.aggregator;

import java.util.ArrayList;
import java.util.List;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@ToString(of = { "aggregations" })
@EqualsAndHashCode(of = { "aggregations" })
public class AggregationGroup {
    @Getter
    private final List<Aggregation> aggregations;

    @Getter
    private final long width;

    public AggregationGroup(List<Aggregation> aggregations) {
        this.aggregations = aggregations;
        this.width = calculateWidth(aggregations);
    }

    private long calculateWidth(List<Aggregation> aggregations) {
        long max = 0;

        for (final Aggregation aggregation : aggregations) {
            final long hint = aggregation.getWidth();

            if (hint > max) {
                max = hint;
            }
        }

        return max;
    }

    public AggregatorGroup build() {
        final List<Aggregator> aggregators = new ArrayList<Aggregator>(aggregations.size());

        for (final Aggregation aggregation : aggregations) {
            aggregators.add(aggregation.build());
        }

        return new AggregatorGroup(this, aggregators);
    }
}