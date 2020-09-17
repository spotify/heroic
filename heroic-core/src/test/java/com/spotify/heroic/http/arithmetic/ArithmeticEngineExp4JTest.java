package com.spotify.heroic.http.arithmetic;


import static java.util.stream.Collectors.toList;

import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.Arithmetic;
import com.spotify.heroic.metric.CacheInfo;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.QueryError;
import com.spotify.heroic.metric.QueryMetricsResponse;
import com.spotify.heroic.metric.QueryResult;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.QueryTrace.ActiveTrace;
import com.spotify.heroic.metric.QueryTrace.Identifier;
import com.spotify.heroic.metric.ResultLimits;
import com.spotify.heroic.metric.ShardedResultGroup;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

// Plan: pull a QueryResult /Map<String, QueryMetricsResponse> off the wire
// using Chrome to hit the endpoint. Could even just hit prod using grafana?
// Nah, that... maybe...
public class ArithmeticEngineExp4JTest {

    private static final int TEST_THRESHOLD = 38;
    public static final long CADENCE = 60L;
    private QueryMetricsResponse queryMetricsResponse;

    @BeforeClass
    void beforeClass() {
        final var metrics = MetricCollection.build(MetricType.POINT, pointsRange(0, 100));

        createSeries(0);

        final var resultGroup = new ShardedResultGroup(
            createStringMap("shard", 1, 3),
            createStringMap("key", 4, 3),
            createSerieses(3),
            metrics,
            CADENCE);

        final var resultGroups = List.of(resultGroup);

        final var errors = List.of(new QueryError("error1"));

        final QueryTrace trace = ActiveTrace.create(Identifier.create("123"),
            DateTime.now().getMillis(), List.of());

        final ResultLimits limits = new ResultLimits();

        final long preAggregationSampleSize = 1000L;

        final Optional<CacheInfo> cache = Optional.empty();

        final DateRange range = DateRange.create(0L, 0L);
        final var qr = new QueryResult(range, resultGroups,
            errors, trace, limits, preAggregationSampleSize, cache);

        final Statistics statistics = Statistics.empty();

        final var uuid = UUID.randomUUID();

        queryMetricsResponse = new QueryMetricsResponse(uuid, range, resultGroups, errors, trace,
            limits, Optional.of(preAggregationSampleSize), cache);
    }

    private Set<Series> createSerieses(int numSeries) {
        return IntStream.range(1, numSeries + 1)
            .mapToObj(i -> createSeries(i))
            .collect(Collectors.toUnmodifiableSet());
    }

    private static Series createSeries(int startingIndex) {
        var tagPairs = createStringMap("tag", startingIndex, 3);
        var resourcePairs = createStringMap("resource", startingIndex + 3, 3);

        return Series.of("seriesKey" + startingIndex, tagPairs, resourcePairs);
    }

    private static ImmutableMap<String, String> createStringMap(String prefix, int startingIndex,
        int numEntries) {

        var collector = ImmutableMap.toImmutableMap(key -> prefix + "Name" + key,
            val -> prefix + "Name" + val);

        return IntStream
            .range(startingIndex, numEntries)
            .mapToObj(i -> prefix + i + 1)
            .collect(collector);
    }

    @Before
    void before() {
    }

    @Test
    void somethingTest1() {
        var responses = Map.of("A", this.queryMetricsResponse);
        var engine = new ArithmeticEngineExp4J(new Arithmetic("fred"), responses);

    }


    /**
     * Create POINT_RANGE_END - POINT_RANGE_START Point objects with sequentially increasing
     * timestamps and values.
     *
     * @return
     */
    private static List<Point> pointsRange(int start, int end) {
        return IntStream.range(start, end).mapToObj(i -> new Point(i, i)).collect(toList());
    }

}
