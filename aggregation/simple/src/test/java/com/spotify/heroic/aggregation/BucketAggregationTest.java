package com.spotify.heroic.aggregation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Data;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Sampling;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;

public class BucketAggregationTest {
    public final class IterableBuilder {
        final ArrayList<Point> datapoints = new ArrayList<Point>();

        public IterableBuilder add(long timestamp, double value) {
            datapoints.add(new Point(timestamp, value));
            return this;
        }

        public List<Point> result() {
            return datapoints;
        }
    }

    public IterableBuilder build() {
        return new IterableBuilder();
    }

    @Data
    public static class TestBucket implements Bucket<Point> {
        private final long timestamp;
        private double sum;

        public void update(Map<String, String> tags, MetricType type, Point d) {
            sum += d.getValue();
        }

        @Override
        public long timestamp() {
            return timestamp;
        }
    }

    public BucketAggregation<Point, TestBucket> setup(Sampling sampling) {
        return new BucketAggregation<Point, TestBucket>(sampling, Point.class, MetricType.POINT) {
            @Override
            protected TestBucket buildBucket(long timestamp) {
                return new TestBucket(timestamp);
            }

            @Override
            protected Point build(TestBucket bucket) {
                return new Point(bucket.timestamp, bucket.sum);
            }
        };
    }

    final Map<String, String> group = ImmutableMap.of();
    final Set<Series> series = ImmutableSet.of();
    final List<AggregationState> states = ImmutableList.of();

    @Test
    public void testSameSampling() {
        final BucketAggregation<Point, TestBucket> a = setup(new Sampling(1000, 1000));
        final AggregationSession session = a.session(states, new DateRange(1000, 3000)).getSession();
        session.update(group(build().add(1000, 50.0).add(1000, 50.0).add(2001, 50.0).result()));

        final AggregationResult result = session.result();

        Assert.assertEquals(new Statistics.Aggregator(3, 0, 0), result.getStatistics());
        Assert.assertEquals(ImmutableList.of(group(build().add(2000, 100.0).add(3000, 50.0).result())),
                result.getResult());
    }

    @Test
    public void testShorterExtent() {
        final BucketAggregation<Point, TestBucket> a = setup(new Sampling(1000, 500));
        final AggregationSession session = a.session(states, new DateRange(1000, 3000)).getSession();
        session.update(group(build().add(1000, 50.0).add(2499, 50.0).add(2500, 50.0).result()));

        final AggregationResult result = session.result();

        Assert.assertEquals(new Statistics.Aggregator(3, 0, 0), result.getStatistics());
        Assert.assertEquals(ImmutableList.of(group(build().add(2000, 0.0).add(3000, 50.0).result())),
                result.getResult());
    }

    @Test
    public void testUnevenSampling() {
        final BucketAggregation<Point, TestBucket> a = setup(new Sampling(999, 499));
        final AggregationSession session = a.session(states, new DateRange(1000, 3000)).getSession();
        session.update(group(build().add(999, 50.0).add(999, 50.0).add(2598, 50.0).result()));

        final AggregationResult result = session.result();

        Assert.assertEquals(new Statistics.Aggregator(3, 0, 0), result.getStatistics());
        Assert.assertEquals(ImmutableList.of(group(build().add(1998, 0.0).add(2997, 50.0).result())),
                result.getResult());
    }

    private AggregationData group(List<Point> values) {
        return new AggregationData(group, series, values, MetricType.POINT);
    }
}
