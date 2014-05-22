package com.spotify.heroic.aggregator;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class AggregationGroupSerializerTest {
    private static final AggregationGroupSerializer serializer = AggregationGroupSerializer.get();

    private AggregationGroup roundTrip(AggregationGroup aggregationGroup) {
        return serializer.fromByteBuffer(serializer.toByteBuffer(aggregationGroup));
    }

    @Test
    public void testEmpty() throws Exception {
        final AggregationGroup aggregationGroup = new AggregationGroup(new ArrayList<Aggregation>());
        Assert.assertEquals(aggregationGroup, roundTrip(aggregationGroup));
    }

    @Test
    public void testSome() throws Exception {
        final Aggregation aggregation = new SumAggregation();
        final List<Aggregation> aggregations = new ArrayList<Aggregation>();
        aggregations.add(aggregation);
        final AggregationGroup aggregationGroup = new AggregationGroup(aggregations);
        Assert.assertEquals(aggregationGroup, roundTrip(aggregationGroup));
    }
}
