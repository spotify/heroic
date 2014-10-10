package com.spotify.heroic.aggregation;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;

import org.junit.Assert;
import org.junit.Test;

import com.spotify.heroic.aggregation.Aggregation.Result;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Sampling;
import com.spotify.heroic.model.Statistics;

public class BucketAggregationTest {
    public final class IterableBuilder {
        final ArrayList<DataPoint> datapoints = new ArrayList<DataPoint>();

        public IterableBuilder add(DataPoint d) {
            datapoints.add(d);
            return this;
        }

        public Iterable<DataPoint> result() {
            return datapoints;
        }
    }

    public IterableBuilder build() {
        return new IterableBuilder();
    }

    @Data
    public static class TestBucket implements Bucket {
        private final long timestamp;
        private double sum;

        public void update(DataPoint d) {
            sum += d.getValue();
        }

        @Override
        public long timestamp() {
            return timestamp;
        }
    }

    public BucketAggregation<TestBucket> setup(Sampling sampling) {
        return new BucketAggregation<TestBucket>(sampling) {
            @Override
            protected TestBucket buildBucket(long timestamp) {
                return new TestBucket(timestamp);
            }

            @Override
            protected DataPoint build(TestBucket bucket) {
                return new DataPoint(bucket.timestamp, bucket.sum);
            }
        };
    }

    @Test
    public void testSameSampling() {
        final BucketAggregation<TestBucket> a = setup(new Sampling(1000, 1000));
        final Aggregation.Session session = a.session(new DateRange(1000, 3000));
        session.update(build().add(new DataPoint(1000, 50.0)).add(new DataPoint(1000, 50.0))
                .add(new DataPoint(2000, 50.0)).result());

        final Result result = session.result();

        Assert.assertEquals(new Statistics.Aggregator(3, 0, 0), result.getStatistics());

        final List<DataPoint> d = result.getResult();
        Assert.assertEquals(2, d.size());
        Assert.assertEquals(new DataPoint(2000, 100.0), d.get(0));
        Assert.assertEquals(new DataPoint(3000, 50.0), d.get(1));
    }

    @Test
    public void testShorterExtent() {
        final BucketAggregation<TestBucket> a = setup(new Sampling(1000, 500));
        final Aggregation.Session session = a.session(new DateRange(1000, 3000));
        session.update(build().add(new DataPoint(1000, 50.0)).add(new DataPoint(2499, 50.0))
                .add(new DataPoint(2500, 50.0)).result());

        final Result result = session.result();

        Assert.assertEquals(new Statistics.Aggregator(3, 0, 0), result.getStatistics());

        final List<DataPoint> d = result.getResult();
        Assert.assertEquals(2, d.size());
        Assert.assertEquals(new DataPoint(2000, Double.NaN), d.get(0));
        Assert.assertEquals(new DataPoint(3000, 50.0), d.get(1));
    }

    @Test
    public void testUnevenSampling() {
        final BucketAggregation<TestBucket> a = setup(new Sampling(999, 499));
        final Aggregation.Session session = a.session(new DateRange(1000, 3000));
        session.update(build().add(new DataPoint(999, 50.0)).add(new DataPoint(999, 50.0))
                .add(new DataPoint(2598, 50.0)).result());

        final Result result = session.result();

        Assert.assertEquals(new Statistics.Aggregator(3, 0, 0), result.getStatistics());

        final List<DataPoint> d = result.getResult();
        Assert.assertEquals(2, d.size());
        Assert.assertEquals(new DataPoint(1998, Double.NaN), d.get(0));
        Assert.assertEquals(new DataPoint(2997, 50.0), d.get(1));
    }
}
