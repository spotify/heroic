/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.http.arithmetic;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.metric.Arithmetic;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.QueryError;
import com.spotify.heroic.metric.QueryMetricsResponse;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.RequestError;
import com.spotify.heroic.metric.ResultLimits;
import com.spotify.heroic.metric.ShardedResultGroup;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import net.objecthunter.exp4j.Expression;
import net.objecthunter.exp4j.ExpressionBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

public class ArithmeticEngineExp4J implements ArithmeticEngine {

    private static final Logger log =
        org.slf4j.LoggerFactory.getLogger(ArithmeticEngineExp4J.class);

    private final Arithmetic arithmetic;
    private final Map<String, QueryMetricsResponse> queryResponses;
    private final UUID uuid;
    private final Expression expressionEngine;

    public ArithmeticEngineExp4J(
        Arithmetic arithmetic, Map<String, QueryMetricsResponse> queryResponses) {
        this.arithmetic = arithmetic;
        this.queryResponses = queryResponses;

        this.uuid = UUID.randomUUID();
        expressionEngine = createExpression();
    }

    public QueryMetricsResponse run() {
        try {
            return evaluateExpression();
        } catch (IllegalArgumentException e) {
            log.info(String.format("Caller supplied arithmetic expression was invalid: %s",
                e.getMessage()));
            return createErrorResponse(e.getMessage());
        }
    }

    /**
     * For each variable <code>name</code> (i.e. the name of a Series e.g. "A"), get the response
     * (and its results) pertaining to it.
     * <p>
     * For each of those response's results, get its key and insert it into <code>resultMap</code>,
     * and add (<code>name</code>, result) to the <code>List<ShardedResultGroup></code> that that
     * key is mapped to.
     * <p>
     * For example, if:
     * <pre>
     * variableNames = [A, B]
     * </pre>
     * and:
     * <pre>
     * queryResponses =
     * {
     *     {A → {response 1: {result 1:{key:P}, result 2:{key:Q} } } },
     *     {B → {response 2: {result 3:{key:P}, result 4:{key:S} } } }
     * }
     * </pre>
     * Then <code>resultMap</code> would look like:
     * <pre>
     * {
     *     P → [ ( A, result 1 ), ( B, result 3 ) ],
     *     Q → [ ( A, result 2 ) ],
     *     S → [ ( B, result 4 ) ]
     * }
     * </pre>
     * So in essence, it plucks out the keys of the result groups and indexes on them, so that the
     * variable names and the results that pertain to them can be found easily.
     * <p>
     *
     * @return NOTE that this is package private for Unit Testing purposes.
     */
    Map<String, ResultList> createResultsMap() {

        final var variableNames = expressionEngine.getVariableNames();

        var resultMap = new HashMap<String, ResultList>();

        variableNames
            .forEach(name -> {

                final var response = queryResponses.get(name);
                final var shardedResultGroups = response.getResult();

                shardedResultGroups
                    .forEach(group -> {
                        final String key = group.getKey().toString();

                        if (!resultMap.containsKey(key)) {
                            resultMap.put(key, new ResultList());
                        }

                        resultMap.get(key).add(Pair.of(name, group));
                    });
            });

        return resultMap;
    }

    /**
     * return true iff, for this query, the number of series returned and the number of variables
     * match and they are non-zero in size.
     * <p>
     * Of course, they might be the "wrong" variable names e.g. where arithmetic = "A / B" and
     * variables = "[X, Y]" but we let the exp4j library discover that.
     * <p>
     * TODO given that exp4j will validate this anyway, is there really a need for this function?
     * Shouldn't the whole query just fail as a whole if the variables are messed-up?
     *
     * @param evaluateEntry query name and its results
     * @param variableNames all variables supposedly in the query
     * @param errors        populate this with any missing variable names
     * @return true iff variables & series' match in size
     * <p>
     * NOTE that this is package private for Unit Testing purposes.
     */
    static boolean doNumOfSeriesAndVariablesMatch(
        final Map.Entry<String, ResultList> evaluateEntry,
        final Set<String> variableNames,
        final List<RequestError> errors) {
        if (evaluateEntry.getValue().size() ==
            variableNames.size() && evaluateEntry.getValue().size() > 0) {
            return true;
        } else {
            final var variables = new HashSet<>(variableNames);

            variables.removeAll(
                evaluateEntry
                    .getValue()
                    .stream()
                    .map(Pair::getLeft)
                    .collect(Collectors.toSet()));

            final String variablesSetRep = StringUtils.join(",", variables);

            // TODO but what about when there are too many variables? This will fail that situation
            errors.add(new QueryError(String.format("Missing entries for variables %s and key "
                + "%s", variablesSetRep, evaluateEntry.getKey())));

            return false;
        }
    }

    /**
     * Validate that the series' are all of equal length.
     *
     * @param evaluateEntry query to evaluate
     * @param errors        this is populated should they not match in length
     * @return true iff all series are of equal length
     * <p>
     * NOTE that this is package private for Unit Testing purposes.
     */
    static boolean areResultSeriesOfEqualLength(
        final Map.Entry<String, ResultList> evaluateEntry,
        final List<RequestError> errors) {

        final ResultList results = evaluateEntry.getValue();

        // For each ShardedResultGroup in the list, get the number of metrics it has
        // size and add that number to a set. We do this because If all the series
        // have the same number of points, the set must have exactly 1 element.
        final Set<Integer> sizes =
            results
                .stream()
                .map(entry -> entry.getRight().getMetrics().size())
                .collect(Collectors.toSet());

        if (sizes.size() == 1) {
            return true;
        } else {
            errors.add(new QueryError(String.format("All series results must return the same "
                + "number of points. Try adding a time "
                + "resolution. Key: %s", evaluateEntry.getKey())));
            return false;
        }
    }

    /**
     * TODO
     *
     * @param evaluateEntry
     * @param errors
     * @return NOTE that this is package private for Unit Testing purposes.
     */
    static boolean checkSeriesTimestamps(
        final Map.Entry<String, ResultList> evaluateEntry,
        final List<RequestError> errors) {

        final ResultList results = evaluateEntry.getValue();

        final Set<Boolean> timestampEqualitySet =
            // For i = 0 to len(first result.metrics)...
            IntStream.range(0, results.get(0).getRight().getMetrics().size())
                // pluck out the ith timestamp
                .mapToObj(i -> {
                    return results.stream().map(entry ->
                        entry
                            .getRight()
                            .getMetrics()
                            .getDataAs(Point.class).get(i).getTimestamp())
                        // and then stick them into a set and then return true if they
                        // are all the same size, false otherwise.
                        .collect(Collectors.toSet()).size() == 1;
                    // Then take all those boolean values and put them into a set
                }).collect(Collectors.toSet());

        // Iff the ith, jth, kth ... timestamps are all equal, then we expect a single true value
        // because size > 1 implies one or more were equal and one or more were unequal and a single
        // false value means none of the ith, jth, kth... timestamps were equal.
        if (timestampEqualitySet.size() > 1 || timestampEqualitySet.contains(false)) {
            errors.add(new QueryError(String.format("All timestamps for all groups must match. "
                + "Key: %s", evaluateEntry.getKey())));
            return false;
        } else {
            return true;
        }
    }

    /**
     * Returns true iff the number of series' and variables matches AND all the series' are of equal
     * length.
     *
     * @param query         query to evaluate
     * @param variableNames unique list of variable names given in query
     * @param errors        will be populated if any of the criteria don't hold
     * @return true iff the number of series' and variables matches AND all the series' are of equal
     * length.
     * <p>
     * NOTE that this is package private for Unit Testing purposes.
     */
    static boolean filterBadSeries(
        final Map.Entry<String, ResultList> query,
        final Set<String> variableNames,
        final List<RequestError> errors) {

        if (!doNumOfSeriesAndVariablesMatch(
            query,
            variableNames,
            errors)) {
            return false;
        }

        if (!areResultSeriesOfEqualLength(query, errors)) {
            return false;
        }

        return checkSeriesTimestamps(query, errors);
    }

    /**
     * TODO Why aren't we doing this AFTER all the sharded data has been merged into one?
     * <p>
     * Say that there are 10 ShardedResultGroup's. This will pluck out the ith
     *
     * @param i            index of series' to extract
     * @param queryResults contains the series in question
     * @return map of the variable names to variable values
     * <p>
     * NOTE that this is package private for Unit Testing purposes.
     */
    @NotNull
    static Map<String, Double> getVariableToValueMapForIthMetric(final int i,
        final ResultList queryResults) {

        /*
            Extract the double value from the ith Point of each ShardedResultGroup.
            So if we were passed i=2 and seriesEntries =
            [
                [A, Group:{..., [1,2,3,4]}],
                [B, Group:{..., [6,7,8,8.5]}],
                [C, Group:{..., [9,10,11,12]}]
            ]

            we would end up with:

            [ [A,3], [B,8], [C,11] ]
        */
        final var queryNamesToMetricValues = queryResults.stream().map(entry -> {
            final String name = entry.getLeft();
            final ShardedResultGroup resultGroup = entry.getRight();

            return new SimpleEntry<>(name, resultGroup.getMetrics().getIthPointValue(i));
        });

        return queryNamesToMetricValues.collect(Collectors.toMap(
            Map.Entry::getKey,
            Map.Entry::getValue));
    }

    /**
     * This is where the rubber meets the road - where the specified arithmetic operation is
     * translated from the human to the computer's domain.
     * <p>
     * The ith Point in each of the input Series' is operated upon (together) to produce a resulting
     * single ith Point object, which is then returned in the result object (the `metrics`
     * property).
     *
     * @param results
     * @param expressionEngine
     * @return a ShardedResultGroup with `metrics` (Point objects) that are the result of applying
     * the arithmetic expression to each corresponding Point in `results`.
     * <p>
     * NOTE that this is package private for Unit Testing purposes.
     */
    @NotNull
    static ShardedResultGroup applyArithmetic(
        final ResultList results,
        final Expression expressionEngine) {

        final var sampleShardGroup = results.get(0).getRight();

        final var sampleShardGroupPoints =
            sampleShardGroup.getMetrics().getDataAs(Point.class);

        /*
        TODO This just seems the wrong way to do this. This is iterating over N elements, reaching
        into each of M Lists each iteration (where M is surely way, way smaller than N), converting
        M elements at a time from Point to double (see call to getElementsAt).
        ... TODO read this further as it might be necessary.

        Surely it's more efficient to iterate all N elements M times, in parallel?
         */
        final var resultsPoints =

            // for int i = 0 to i = (size - 1)
            IntStream.range(0, sampleShardGroupPoints.size())

                // convert each int to a Point object
                .mapToObj(i -> {
                    // pluck out the query's variable names (e.g. ['A', 'B'] for Series A and B)
                    // and values for the ith element of all Series and end up with e.g. [A=2
                    // .425,B=0.923]
                    final var variableMap = getVariableToValueMapForIthMetric(i, results);

                    // plug the variables and their values into the engine and evaluate the result
                    final double result = expressionEngine.setVariables(variableMap).evaluate();

                    // TODO bug alert: what if the timestamps aren't lined up? We'll be assigning
                    //  the wrong timestamp here
                    return new Point(sampleShardGroupPoints.get(i).getTimestamp(), result);

                }).collect(Collectors.toList());

        return new ShardedResultGroup(
            sampleShardGroup.getShard(),
            sampleShardGroup.getKey(),
            sampleShardGroup.getSeries(),
            MetricCollection.points(resultsPoints),
            sampleShardGroup.getCadence());
    }

    @NotNull
    private QueryMetricsResponse evaluateExpression() {

        final var resultsMap = createResultsMap();

        final List<RequestError> errors = new ArrayList<RequestError>();

        final List<ShardedResultGroup> results = resultsMap
            .entrySet()
            .stream()
            .filter(predicate -> {
                // `query` is created solely so that we can pass a ResultList object to
                // filterBadSeries.
                var query = new SimpleEntry<>(
                    predicate.getKey(), (ResultList) predicate.getValue());

                return filterBadSeries(query, expressionEngine.getVariableNames(), errors);
            })
            .map(mapper -> {
                return applyArithmetic((ResultList) mapper.getValue(), expressionEngine);
            }).collect(Collectors.toList());

        return createQueryMetricsResponse(
            queryResponses.values().stream().findFirst().get().getRange(),
            "success",
            results,
            ImmutableList.of()
        );
    }

    @Contract("_ -> new")
    @NotNull
    private QueryMetricsResponse createErrorResponse(String message) {

        return createQueryMetricsResponse(DateRange.create(0L, 1L), message,
            ImmutableList.of(), ImmutableList.of(new QueryError(
                String.format("Expression '%s' is invalid: %s", arithmetic.getExpression(),
                    message))));
    }

    @Contract("_, _, _, _ -> new")
    @NotNull
    private QueryMetricsResponse createQueryMetricsResponse(
        DateRange range, String message, List<ShardedResultGroup> results,
        List<QueryError> errors) {
        return new QueryMetricsResponse(
            uuid,
            range,
            results,
            errors,
            QueryTrace.PASSIVE,
            ResultLimits.of(),
            Optional.empty(),
            Optional.empty()
        );
    }

    private Expression createExpression() {

        final var exp = arithmetic.getExpression();

        final var expression = new ExpressionBuilder(
            exp)
            .variables(queryResponses.keySet())
            .build();

        return expression;
    }

    Arithmetic getArithmetic() {
        return arithmetic;
    }

    UUID getUuid() {
        return uuid;
    }
}
