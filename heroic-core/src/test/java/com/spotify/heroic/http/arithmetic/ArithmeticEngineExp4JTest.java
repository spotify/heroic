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
import com.spotify.heroic.metric.RequestError;
import com.spotify.heroic.metric.ResultLimits;
import com.spotify.heroic.metric.ShardedResultGroup;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

// Plan: pull a QueryResult /Map<String, QueryMetricsResponse> off the wire
// using Chrome to hit the endpoint. Could even just hit prod using grafana?
// Nah, that... maybe...
@RunWith(MockitoJUnitRunner.class)
public class ArithmeticEngineExp4JTest extends TestCase {

    public static final int NUM_POINTS = 100;
    private static int TEST_THRESHOLD = 38;
    public static long CADENCE = 60L;
    private static QueryMetricsResponse queryMetricsResponse;

    @BeforeClass
    public static void beforeClass() {
        var metrics = MetricCollection.build(MetricType.POINT, pointsRange(0, NUM_POINTS));

        createSeries(0);

        ShardedResultGroup resultGroup = createDefaultResultGroup(metrics);

        var resultGroups = List.of(resultGroup);

        queryMetricsResponse = createMetricsResponse(resultGroups);
    }

    @Before
    public void before() {
    }

    @Test
    public void testBadInputWrongVariableName() {
        var responses = Map.of("A", this.queryMetricsResponse);
        var operator = new SeriesArithmeticOperator(new Arithmetic("fred"), responses);

        var response = operator.reduce();
        performBadInputAssertions(response);
        var error = (QueryError) response.getErrors().get(0);

        // "fred" isn't a variable in any of the input responses ∴ it's an illegal argument
        var expectedError = new QueryError("Expression 'fred' is invalid: "
            + "Unknown function or variable 'fred' at pos 0 in expression 'fred'");
        assertEquals(expectedError, error);
    }

    @Test
    public void testBadInputExtraVariableName() {
        var responses = Map.of("A", this.queryMetricsResponse);
        var operator = new SeriesArithmeticOperator(new Arithmetic("5 * A * fred"),
            responses);

        var response = operator.reduce();
        performBadInputAssertions(response);

        var error = (QueryError) response.getErrors().get(0);

        // "fred" isn't a variable in any of the input responses ∴ it's an illegal argument
        var expectedError = new QueryError("Expression '5 * A * fred' is invalid: "
            + "Unknown function or variable 'fred' at pos 8 in expression '5 * A * fred'");
        assertEquals(expectedError, error);
    }

    @Test
    public void testBadInputDivideByZero() {

        var responses = Map.of("A", this.queryMetricsResponse);
        var operator = new SeriesArithmeticOperator(new Arithmetic("5 * A / 0"),
            responses);

        var response = operator.reduce();
        var points = response.getResult().get(0).getMetrics();
        var expectedPoints = MetricCollection.build(MetricType.POINT,
            IntStream
                .range(0, NUM_POINTS)
                .mapToObj(i -> new Point(i, 0))
                .collect(toList()));

        assertEquals(expectedPoints, points);

        // TODO : update this when applyArithmetic() has been corrected to return
        //  errors for the !DIV/0
        assertEquals(new ArrayList<RequestError>(), response.getErrors());
    }

    @Test
    public void testBadArithmeticExpression() {
        checkBadArithmeticExpression("5 + A / / A 5");
        checkBadArithmeticExpression("5 5");
        checkBadArithmeticExpression("A B *");
        checkBadArithmeticExpression("*AB");
        checkBadArithmeticExpression("A / B /");
        checkBadArithmeticExpression("A */ B");
        checkBadArithmeticExpression("* A / B");

        // TODO when anything but division is disallowed, this will need updating
    }

    private void checkBadArithmeticExpression(String expression) {
        var responses = Map.of(
            "A", this.queryMetricsResponse, "B", this.queryMetricsResponse);
        var operator = new SeriesArithmeticOperator(new Arithmetic(expression),
            responses);

        var response = operator.reduce();
        performBadInputAssertions(response);

        var finalResponse = operator.reduce();
        var error = (QueryError) finalResponse.getErrors().get(0);
        assertTrue(error.getError().toLowerCase().contains("is invalid: "));
    }

    @Test
    public void testEmptyArithmeticExpression() {

        var responses = Map.of("A", this.queryMetricsResponse);
        var operator = new SeriesArithmeticOperator(new Arithmetic(""),
            responses);

        var finalResponse = operator.reduce();
        var error = (QueryError) finalResponse.getErrors().get(0);
        assertEquals("Expression '' is invalid: Expression can not be empty", error.getError());
    }


    @Test
    public void testMissingVariableName() {
        var responses = new ArrayList<QueryMetricsResponse>();
        var metrics = MetricCollection.build(MetricType.POINT, pointsRange(0, NUM_POINTS));

        responses.add(createMetricsResponse(new ArrayList<ShardedResultGroup>() {
            {
                add(
                    createDefaultResultGroup(metrics));
            }
        }));
        responses.add(createMetricsResponse(new ArrayList<ShardedResultGroup>() {
            {
                add(
                    createDefaultResultGroup(metrics));
            }
        }));

        // Note that "B" Series is missing
        var queryNameToResponse = Map.of("A", responses.get(0), "C", responses.get(1));
        var operator = new SeriesArithmeticOperator(new Arithmetic("A / B"), queryNameToResponse);

        var finalResponse = operator.reduce();
        var error = (QueryError) finalResponse.getErrors().get(0);
        assertEquals("Expression 'A / B' is invalid: Unknown function or variable 'B' at pos 4 in"
            + " expression 'A / B'", error.getError());
    }

    @Test
    public void testTooManySerieses() {
        var metrics = MetricCollection.build(MetricType.POINT, pointsRange(0, NUM_POINTS));

        var lotsOfResponses = IntStream.range(0, 1000).mapToObj(
            i -> createMetricsResponse(new ArrayList<ShardedResultGroup>() {
                {
                    add(
                        createDefaultResultGroup(metrics));
                }
            })
        ).collect(Collectors.toList());

        int count = 0;
        var queryNameToResponse = new HashMap<String, QueryMetricsResponse>(1000);
        for (var r : lotsOfResponses) {
            queryNameToResponse.put("Series_" + String.valueOf(++count), r);
        }

        // TODO add tests for "too many series" once that check has been coded up
        //  in ArithmeticEngineExp4J
    }

    @Test
    public void testZeroLengthSerieses() {
        var responses = new ArrayList<QueryMetricsResponse>();
        var metrics = MetricCollection.build(MetricType.POINT, pointsRange(0, 0));

        responses.add(createMetricsResponse(
            new ArrayList<ShardedResultGroup>() {{
                add(createDefaultResultGroup(metrics));
            }}));

        var queryNameToResponse = Map.of("A", responses.get(0));
        var operator = new SeriesArithmeticOperator(new Arithmetic("A * 100.5"),
            queryNameToResponse);

        var finalResponse = operator.reduce();
        assertEquals(0, finalResponse.getResult().get(0).getMetrics().size());
        assertEquals(0, finalResponse.getErrors().size());
    }

    @Test
    public void testUnequalLengthSerieses() {
        var allResponses = new ArrayList<QueryMetricsResponse>();

        {
            // one shorter than the next result
            var metrics = MetricCollection.build(MetricType.POINT, pointsRange(0, 99));

            // TODO How can there be 3 series and 1 metrics List?
            allResponses.add(createMetricsResponse(new ArrayList<ShardedResultGroup>() {
                {
                    add(
                        createDefaultResultGroup(metrics));
                }
            }));
        }

        {
            var metrics = MetricCollection.build(MetricType.POINT, pointsRange(0, NUM_POINTS));

            allResponses.add(createMetricsResponse(new ArrayList<ShardedResultGroup>() {
                {
                    add(
                        createDefaultResultGroup(metrics));
                }
            }));
        }

        var queryNameToResponse = Map.of("A", allResponses.get(0), "B", allResponses.get(1));
        var operator = new SeriesArithmeticOperator(new Arithmetic("A / B"), queryNameToResponse);

        var finalResponse = operator.reduce();
        var error = (QueryError) finalResponse.getErrors().get(0);
        assertTrue(error.getError().contains("All series results must return the same number of "
            + "points"));
    }

    @NotNull
    private static ShardedResultGroup createDefaultResultGroup(MetricCollection metrics) {
        return new ShardedResultGroup(
            createStringMap("shard", 1, 3),
            createStringMap("key", 4, 3),
            createSerieses(3),
            metrics,
            CADENCE);
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

    /**
     * Creates a map that looks like:
     * <pre>
     * {
     *   prefixKey1 -> prefixVal1,
     *   prefixKey2 -> prefixVal2,
     *   ...
     * }
     * </pre>
     *
     * @param prefix        any string identifier e.g. "tag"
     * @param startingIndex what number to start counting up from
     * @param numEntries    how big the map should be
     * @return see method Javadoc above
     */
    private static ImmutableMap<String, String> createStringMap(String prefix, int startingIndex,
        int numEntries) {

        var collector = ImmutableMap.toImmutableMap(key -> prefix + "Key" + key.toString(),
            val -> prefix + "Val" + val.toString());

        return IntStream
            .range(startingIndex, startingIndex + numEntries)
            .mapToObj(i -> (i + 1))
            .collect(collector);
    }

    private static QueryMetricsResponse createMetricsResponse(List<ShardedResultGroup> resultGroups) {
        var errors = List.of(new QueryError("error1"));

        QueryTrace trace = ActiveTrace.create(Identifier.create("123"),
            DateTime.now().getMillis(), List.of());

        ResultLimits limits = new ResultLimits();

        long preAggregationSampleSize = 1000L;

        Optional<CacheInfo> cache = Optional.empty();

        DateRange range = DateRange.create(0L, 0L);

        var uuid = UUID.randomUUID();

        return new QueryMetricsResponse(uuid, range, resultGroups, errors, trace,
            limits, Optional.of(preAggregationSampleSize), cache);
    }


}
