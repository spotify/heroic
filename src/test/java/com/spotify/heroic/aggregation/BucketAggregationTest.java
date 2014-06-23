package com.spotify.heroic.aggregation;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.spotify.heroic.aggregation.Aggregation.Result;
import com.spotify.heroic.metrics.model.Statistics;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Sampling;

public class BucketAggregationTest {
    private static final Object REFERENCE = new Object();

    public Iterable<DataPoint> one(final DataPoint d) {
        final ArrayList<DataPoint> datapoints = new ArrayList<DataPoint>();
        datapoints.add(d);
        return datapoints;
    }

    @Test
    public void testStuff() {
        final BucketAggregation a = new BucketAggregation(new Sampling(1000,
                1000)) {
            @Override
            protected DataPoint build(long timestamp, long count, double value,
                    float p) {
                return new DataPoint(timestamp, value, p);
            }
        };

        final Aggregation.Session session = a
                .session(new DateRange(1000, 3000));
        session.update(one(new DataPoint(1000, 50.0, Float.NaN)));
        session.update(one(new DataPoint(1001, 50.0, Float.NaN)));
        session.update(one(new DataPoint(2001, 50.0, Float.NaN)));

        final Result result = session.result();

        Assert.assertEquals(new Statistics.Aggregator(3, 0, 0),
                result.getStatistics());

        final List<DataPoint> d = result.getResult();
        Assert.assertEquals(2, d.size());
        Assert.assertEquals(new DataPoint(1000, 100.0, 1.0f), d.get(0));
        Assert.assertEquals(new DataPoint(2000, 50.0, 0.5f),
                d.get(1));
    }
}
