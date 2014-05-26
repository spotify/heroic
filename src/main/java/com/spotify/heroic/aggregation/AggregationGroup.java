package com.spotify.heroic.aggregation;

import java.util.ArrayList;
import java.util.List;

import com.spotify.heroic.aggregator.Aggregator;
import com.spotify.heroic.aggregator.AggregatorGroup;

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
        if (aggregations.isEmpty())
            return 0;

        final Aggregation last = aggregations.get(aggregations.size() - 1);
        return last.getWidth();
    }

    public AggregatorGroup build() {
        final List<Aggregator> aggregators = new ArrayList<Aggregator>(aggregations.size());

        for (final Aggregation aggregation : aggregations) {
            aggregators.add(aggregation.build());
        }

        return new AggregatorGroup(this, aggregators);
    }
}