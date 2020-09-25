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
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.QueryTrace.ActiveTrace;
import com.spotify.heroic.metric.QueryTrace.Identifier;
import com.spotify.heroic.metric.ResultLimits;
import com.spotify.heroic.metric.ShardedResultGroup;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import junit.framework.TestCase;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

// Plan: pull a QueryResult /Map<String, QueryMetricsResponse> off the wire
// using Chrome to hit the endpoint. Could even just hit prod using grafana?
// Nah, that... maybe...
@RunWith(MockitoJUnitRunner.class)
public class ArithmeticEngineExp4JTest extends TestCase {

    private static final int TEST_THRESHOLD = 38;
    public static final long CADENCE = 60L;
    private static QueryMetricsResponse queryMetricsResponse;

    @BeforeClass
    public static void beforeClass() {
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

        final var uuid = UUID.randomUUID();

        queryMetricsResponse = new QueryMetricsResponse(uuid, range, resultGroups, errors, trace,
            limits, Optional.of(preAggregationSampleSize), cache);
    }

    @Before
    public void before() {
    }

    @Test
    public void testBadInputInvalidVariable() {
        final var responses = Map.of("A", this.queryMetricsResponse);
        final var operator = new SeriesArithmeticOperator(new Arithmetic("fred"), responses);

        var response = operator.reduce();
        performBadInputAssertions(response);
        var error = (QueryError) response.getErrors().get(0);

        // "fred" isn't a variable in any of the input responses ∴ it's an illegal argument
        var expectedError = new QueryError("Expression 'fred' is invalid: "
            + "Unknown function or variable 'fred' at pos 0 in expression 'fred'");
        assertEquals(expectedError, error);
    }

    @Test
    public void testBadInputExtraVariable() {
        final var responses = Map.of("A", this.queryMetricsResponse);
        final var operator = new SeriesArithmeticOperator(new Arithmetic("5 * A * fred"),
            responses);

        var response = operator.reduce();
        performBadInputAssertions(response);

        var error = (QueryError) response.getErrors().get(0);

        // "fred" isn't a variable in any of the input responses ∴ it's an illegal argument
        var expectedError = new QueryError("Expression '5 * A * fred' is invalid: "
            + "Unknown function or variable 'fred' at pos 8 in expression '5 * A * fred'");
        assertEquals(expectedError, error);
    }

    @Test(expected = java.lang.ArithmeticException.class)
    public void testBadInputDivideByZero() {

        final var responses = Map.of("A", this.queryMetricsResponse);
        final var operator = new SeriesArithmeticOperator(new Arithmetic("5 * A / 0"),
            responses);

        var response = operator.reduce();
        performBadInputAssertions(response);

        var error = (QueryError) response.getErrors().get(0);

        // "fred" isn't a variable in any of the input responses ∴ it's an illegal argument
        var neverAssignedVariable = new QueryError("Expression 'bananas'");
    }


    @Test
    public void testUnequalLengthSerieses() {
        // TODO
//        final var copiedResponse = queryMetricsResponse
    }


    private static void performBadInputAssertions(QueryMetricsResponse response) {
        assertEquals(Optional.empty(), response.getCache());
        assertEquals(new ArrayList<ShardedResultGroup>(), response.getResult());
        assertEquals(new Statistics(), response.getStatistics());
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

    private static Set<Series> createSerieses(int numSeries) {
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

        var collector = ImmutableMap.toImmutableMap(key -> prefix + "Key" + key.toString(),
            val -> prefix + "Val" + val.toString());

        return IntStream
            .range(startingIndex, startingIndex + numEntries)
            .mapToObj(i -> (i + 1))
            .collect(collector);
    }

}
