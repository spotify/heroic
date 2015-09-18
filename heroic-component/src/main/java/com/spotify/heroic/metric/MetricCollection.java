package com.spotify.heroic.metric;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.spotify.heroic.aggregation.AggregationSession;
import com.spotify.heroic.aggregation.Bucket;
import com.spotify.heroic.common.Series;

import lombok.AccessLevel;
import lombok.Data;
import lombok.RequiredArgsConstructor;

/**
 * A collection of metrics.
 *
 * Metrics are constrained to the implemented types below, so far these are {@link Point}, {@link Event}, {@link Spread}
 * , and {@link MetricGroup}.
 *
 * There is a JSON serialization available in {@link MetricCollectionSerialization} which correctly preserves the type
 * information of these collections.
 *
 * This class is a carrier for _any_ of these metrics, the canonical way for accessing the underlying data is to first
 * check it's type using {@link #getType()}, and then access the data with the appropriate cast using
 * {@link #getDataAs(Class)}.
 *
 * The following is an example for how you may access data from the collection.
 *
 * <pre>
 * final MetricCollection collection = ...;
 *
 * if (collection.getType() == MetricType.POINT) {
 *     final List<Point> points = collection.getDataAs(Point.class);
 *     ...
 * }
 * </pre>
 *
 * @see Point
 * @see Spread
 * @see Event
 * @see MetricGroup
 * @author udoprog
 */
@Data
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public abstract class MetricCollection {
    final MetricType type;
    final List<? extends Metric> data;

    /**
     * Helper method to fetch a collection of the given type, if applicable.
     *
     * @param expected The expected type to read.
     * @return A list of the expected type.
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> getDataAs(Class<T> expected) {
        if (!expected.isAssignableFrom(type.type())) {
            throw new IllegalArgumentException(
                    String.format("Cannot assign type (%s) to expected (%s)", type, expected));
        }

        return (List<T>) data;
    }

    /**
     * Update the given aggregation with the content of this collection.
     */
    public abstract void updateAggregation(final AggregationSession session, final Map<String, String> tags,
            final Set<Series> series);

    /**
     * Update the given bucket with the content of this collection.
     *
     * @param bucket The bucket to update.
     * @param tags
     */
    public abstract void updateBucket(final Bucket bucket, final Map<String, String> tags);

    public boolean isEmpty() {
        return data.isEmpty();
    }

    // @formatter:off
    private static final Map<MetricType, Function<List<? extends Metric>, MetricCollection>> adapters = ImmutableMap.<MetricType, Function<List<? extends Metric>, MetricCollection>>of(
        MetricType.GROUP, GroupCollection::new,
        MetricType.POINT, PointCollection::new,
        MetricType.EVENT, EventCollection::new,
        MetricType.SPREAD, SpreadCollection::new
    );
    // @formatter:on

    public static MetricCollection groups(List<MetricGroup> metrics) {
        return new GroupCollection(metrics);
    }

    public static MetricCollection points(List<Point> metrics) {
        return new PointCollection(metrics);
    }

    public static MetricCollection events(List<Event> metrics) {
        return new EventCollection(metrics);
    }

    public static MetricCollection spreads(List<Spread> metrics) {
        return new SpreadCollection(metrics);
    }

    public static MetricCollection build(final MetricType key, final List<? extends Metric> metrics) {
        final Function<List<? extends Metric>, MetricCollection> adapter = checkNotNull(adapters.get(key), "applier does not exist for type");
        return adapter.apply(metrics);
    }

    public static MetricCollection mergeSorted(final MetricType type, final List<List<? extends Metric>> values) {
        final List<Metric> data = ImmutableList.copyOf(Iterators.mergeSorted(
                ImmutableList.copyOf(values.stream().map(Iterable::iterator).iterator()), type.comparator()));
        return build(type, data);
    }

    @SuppressWarnings("unchecked")
    private static class PointCollection extends MetricCollection {
        PointCollection(List<? extends Metric> points) {
            super(MetricType.POINT, points);
        }

        @Override
        public void updateAggregation(AggregationSession session, Map<String, String> tags, Set<Series> series) {
            session.updatePoints(tags, series, adapt());
        }

        @Override
        public void updateBucket(Bucket bucket, Map<String, String> tags) {
            adapt().forEach((m) -> bucket.updatePoint(tags, m));
        }

        private List<Point> adapt() {
            return (List<Point>) data;
        }
    };

    @SuppressWarnings("unchecked")
    private static class EventCollection extends MetricCollection {
        EventCollection(List<? extends Metric> events) {
            super(MetricType.EVENT, events);
        }

        @Override
        public void updateAggregation(AggregationSession session, Map<String, String> tags, Set<Series> series) {
            session.updateEvents(tags, series, adapt());
        }

        @Override
        public void updateBucket(Bucket bucket, Map<String, String> tags) {
            adapt().forEach((m) -> bucket.updateEvent(tags, m));
        }

        private List<Event> adapt() {
            return (List<Event>) data;
        }
    };

    @SuppressWarnings("unchecked")
    private static class SpreadCollection extends MetricCollection {
        SpreadCollection(List<? extends Metric> spread) {
            super(MetricType.SPREAD, spread);
        }

        @Override
        public void updateAggregation(AggregationSession session, Map<String, String> tags, Set<Series> series) {
            session.updateSpreads(tags, series, adapt());
        }

        @Override
        public void updateBucket(Bucket bucket, Map<String, String> tags) {
            adapt().forEach((m) -> bucket.updateSpread(tags, m));
        }

        private List<Spread> adapt() {
            return (List<Spread>) data;
        }
    };

    @SuppressWarnings("unchecked")
    private static class GroupCollection extends MetricCollection {
        GroupCollection(List<? extends Metric> groups) {
            super(MetricType.GROUP, groups);
        }

        @Override
        public void updateAggregation(AggregationSession session, Map<String, String> tags, Set<Series> series) {
            session.updateGroup(tags, series, adapt());
        }

        @Override
        public void updateBucket(Bucket bucket, Map<String, String> tags) {
            adapt().forEach((m) -> bucket.updateGroup(tags, m));
        }

        private List<MetricGroup> adapt() {
            return (List<MetricGroup>) data;
        }
    };
}