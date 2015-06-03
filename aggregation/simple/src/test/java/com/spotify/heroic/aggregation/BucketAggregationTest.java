package com.spotify.heroic.aggregation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.Data;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.aggregation.Aggregation.Group;
import com.spotify.heroic.aggregation.Aggregation.Result;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Sampling;
import com.spotify.heroic.model.Statistics;

public class BucketAggregationTest {
    public final class IterableBuilder {
        final ArrayList<DataPoint> datapoints = new ArrayList<DataPoint>();

        public IterableBuilder add(long timestamp, double value) {
            datapoints.add(new DataPoint(timestamp, value));
            return this;
        }

        public List<DataPoint> result() {
            return datapoints;
        }
    }

    public IterableBuilder build() {
        return new IterableBuilder();
    }

    @Data
    public static class TestBucket implements Bucket<DataPoint> {
        private final long timestamp;
        private double sum;

        public void update(Map<String, String> tags, DataPoint d) {
            sum += d.getValue();
        }

        @Override
        public long timestamp() {
            return timestamp;
        }
    }

    public BucketAggregation<DataPoint, DataPoint, TestBucket> setup(Sampling sampling) {
        return new BucketAggregation<DataPoint, DataPoint, TestBucket>(sampling, DataPoint.class, DataPoint.class) {
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
        final BucketAggregation<DataPoint, DataPoint, TestBucket> a = setup(new Sampling(1000, 1000));
        final Aggregation.Session session = a.session(DataPoint.class, new DateRange(1000, 3000));
        session.update(group(build().add(1000, 50.0).add(1000, 50.0).add(2001, 50.0).result()));

        final Result result = session.result();

        Assert.assertEquals(new Statistics.Aggregator(3, 0, 0), result.getStatistics());
        Assert.assertEquals(ImmutableList.of(group(build().add(2000, 100.0).add(3000, 50.0).result())),
                result.getResult());
    }

    @Test
    public void testShorterExtent() {
        final BucketAggregation<DataPoint, DataPoint, TestBucket> a = setup(new Sampling(1000, 500));
        final Aggregation.Session session = a.session(DataPoint.class, new DateRange(1000, 3000));
        session.update(group(build().add(1000, 50.0).add(2499, 50.0).add(2500, 50.0).result()));

        final Result result = session.result();

        Assert.assertEquals(new Statistics.Aggregator(3, 0, 0), result.getStatistics());
        Assert.assertEquals(ImmutableList.of(group(build().add(2000, 0.0).add(3000, 50.0).result())),
                result.getResult());
    }

    @Test
    public void testUnevenSampling() {
        final BucketAggregation<DataPoint, DataPoint, TestBucket> a = setup(new Sampling(999, 499));
        final Aggregation.Session session = a.session(DataPoint.class, new DateRange(1000, 3000));
        session.update(group(build().add(999, 50.0).add(999, 50.0).add(2598, 50.0).result()));

        final Result result = session.result();

        Assert.assertEquals(new Statistics.Aggregator(3, 0, 0), result.getStatistics());
        Assert.assertEquals(ImmutableList.of(group(build().add(1998, 0.0).add(2997, 50.0).result())),
                result.getResult());
    }

    private Group group(List<DataPoint> values) {
        return new Group(Group.EMPTY_GROUP, values);
    }
}
