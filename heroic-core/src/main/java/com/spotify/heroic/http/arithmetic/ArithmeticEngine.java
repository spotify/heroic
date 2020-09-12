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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import net.objecthunter.exp4j.Expression;
import net.objecthunter.exp4j.ExpressionBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

public class ArithmeticEngine {

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(ArithmeticEngine.class);

    private static Expression createExpression(@NotNull final Arithmetic arithmetic,
        @NotNull final Map<String, QueryMetricsResponse> resultsMap) {

        if (!arithmetic.getExpression().isPresent()) {
            throw new IllegalArgumentException("No expression was supplied");
        }

        final var expression = new ExpressionBuilder(
            arithmetic
                .getExpression().get())
            .variables(resultsMap.keySet())
            .build();

        return expression;
    }

    public static QueryMetricsResponse run(final Arithmetic arithmetic,
        // String could be "A" from Grafana
        final Map<String, QueryMetricsResponse> queryResults) {

        final UUID newUUID = UUID.randomUUID();

        try {
            final var expression = createExpression(arithmetic, queryResults);

            return evaluateExpression(newUUID, expression,
                queryResults);
        } catch (IllegalArgumentException e) {
            log.info("Caller supplied arithmetic expression was invalid: " + e.getMessage());
            return createErrorResponse(arithmetic, newUUID, e.getMessage());
        }
    }

    @NotNull
    private static QueryMetricsResponse createErrorResponse(Arithmetic arithmetic, UUID newUUID,
        String message) {
        return new QueryMetricsResponse(
            newUUID,
            DateRange.create(0L, 1L),
            ImmutableList.of(),
            ImmutableList.of(new QueryError(
                String.format("Expression '%s' is invalid: %s", arithmetic.getExpression(),
                    message))),
            QueryTrace.PASSIVE,
            ResultLimits.of(),
            Optional.empty(),
            Optional.empty()
        );
    }

    private static ResultMap createResultsMap(
        final Set<String> variableNames,
        final Map<String, QueryMetricsResponse> responseMap) {

        var resultMap = new ResultMap();

        variableNames
            .forEach(name -> {

                final var response = responseMap.get(name);
                final var shardedResultGroups = response.getResult();

                shardedResultGroups
                    .forEach(group -> {
                        final String key = group.getKey().toString();

                        if (!resultMap.containsKey(key)) {
                            resultMap.put(key, new ArrayList<Pair<String, ShardedResultGroup>>());
                        }

                        resultMap.get(key).add(Pair.of(name, group));
                    });
            });

        return resultMap;
    }

    private static boolean checkSeriesCountMatches(
        final Map.Entry<String, List<Pair<String, ShardedResultGroup>>> evaluateEntry,
        final Set<String> variableNames,
        final List<RequestError> errors) {
        if (evaluateEntry.getValue().size() ==
            variableNames.size() && evaluateEntry.getValue().size() > 0) {
            return true;
        } else {
            final Set<String> variableSet = new HashSet<>(variableNames);
            variableSet.removeAll(
                evaluateEntry.getValue().stream().map(Pair::getLeft).collect(Collectors.toSet()));
            final String variablesSetRep = StringUtils.join(",", variableSet);
            errors.add(new QueryError(String.format("Missing entries for variables %s and key "
                    + "%s", variablesSetRep,
                evaluateEntry.getKey())));
            return false;
        }
    }

    private static boolean checkSeriesSizes(
        final Map.Entry<String, List<Pair<String, ShardedResultGroup>>> evaluateEntry,
        final List<RequestError> errors) {
        final List<Pair<String, ShardedResultGroup>> entriesForKey = evaluateEntry.getValue();
        final Set<Integer> sizes =
            entriesForKey.stream().map(entry -> entry.getRight().getMetrics().size()
            ).collect(Collectors
                .toSet());
        if (sizes.size() == 1) {
            return true;
        } else {
            errors.add(new QueryError(String.format("All series results must return the same "
                + "number of points. Try adding a time "
                + "resolution. Key: %s", evaluateEntry.getKey())));
            return false;
        }
    }

    private static boolean checkSeriesTimestamps(
        final Map.Entry<String, List<Pair<String, ShardedResultGroup>>> evaluateEntry,
        final List<RequestError> errors) {
        final List<Pair<String, ShardedResultGroup>> entriesForKey = evaluateEntry.getValue();
        final Set<Boolean> timestampEqualitySet =
            IntStream.range(0, entriesForKey.get(0).getRight().getMetrics().size())
                .mapToObj(i -> {
                    return entriesForKey.stream()
                        .map(entry -> entry.getRight().getMetrics()
                            .getDataAs(Point.class).get(i).getTimestamp())
                        .collect(Collectors.toSet()).size() == 1;

                }).collect(Collectors.toSet());
        if (timestampEqualitySet.size() > 1 || timestampEqualitySet.contains(false)) {
            errors.add(new QueryError(String.format("All timestamps for all groups must match. "
                + "Key: %s", evaluateEntry.getKey())));
            return false;
        } else {
            return true;
        }
    }

    private static boolean filterBadSeries(
        final Map.Entry<String, List<Pair<String, ShardedResultGroup>>> evaluateEntry,
        final Set<String> variableNames,
        final List<RequestError> errors) {
        if (!checkSeriesCountMatches(
            evaluateEntry,
            variableNames,
            errors)) {
            return false;
        }
        if (!checkSeriesSizes(evaluateEntry, errors)) {
            return false;
        }
        return checkSeriesTimestamps(evaluateEntry, errors);
    }

    /**
     * TODO Why aren't we donig this AFTER all the sharded data has been merged into one?
     * <p>
     * Say that there are 10 ShardedResultGroup's. This will pluck out the ith
     *
     * @param i
     * @param seriesEntries
     * @return
     */
    private static Map<String, Double> getVariableToValueMapForIthMetric(final int i,
        final List<Pair<String, ShardedResultGroup>> seriesEntries) {

        /*
            Reduce each ShardedResultGroup to the double value of its ith Point.
            So if we were passed i=2 and seriesEntries =

            [ [A, Group:{[1,2,3,4]}], [B, Group:{[6,7,8,8.5]}], , [C, Group:{[9,10,11,12]}] ]

            we would end up with:

            [ [A, 3], [B,8], [C,11] ]
        */
        final Stream<SimpleEntry<String, Double>> simpleEntryStream =
            seriesEntries.stream().map(entry -> {

                final String name = entry.getLeft();
                final ShardedResultGroup resultGroup = entry.getRight();

                return new SimpleEntry<>(
                    name,
                    resultGroup
                        .getMetrics()
                        // cast the whole series to List<Point>, get the ith Point
                        // and return its double value.
                        .getDataAs(Point.class).get(i).getValue());

            });

        return simpleEntryStream.collect(Collectors.toMap(
            Map.Entry::getKey,
            Map.Entry::getValue));
    }

    /**
     * This is where the rubber meets the road - where the specified arithmetic operation is
     * translated from the human to the computer's domain.
     *
     * @param seriesEntries
     * @param expressionEngine
     * @return
     */
    private static ShardedResultGroup runArithmeticForSeries(
        final List<Pair<String, ShardedResultGroup>> seriesEntries,
        final Expression expressionEngine) {

        final var sampleShardGroup = seriesEntries.get(0).getRight();

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
            // for all points in the series
            IntStream.range(0, sampleShardGroupPoints.size())
                /**
                 * Returns an object-valued {@code Stream} consisting of the results of
                 * applying the given function to the elements of this stream.
                 */
                .mapToObj(i -> {
                    // pluck out the variable names and values for the ith element of all Series
                    final var variableMap = getVariableToValueMapForIthMetric(i, seriesEntries);

                    //
                    final double result = expressionEngine.setVariables(variableMap).evaluate();
                    return new Point(sampleShardGroupPoints.get(i).getTimestamp(), result);
                }).collect(Collectors.toList());

        return new ShardedResultGroup(
            sampleShardGroup.getShard(),
            sampleShardGroup.getKey(),
            sampleShardGroup.getSeries(),
            MetricCollection.points(resultsPoints),
            sampleShardGroup.getCadence());
    }

    private static QueryMetricsResponse evaluateExpression(
        final UUID newUUID, final Expression expressionEngine,
        final Map<String, QueryMetricsResponse> queryResults) {

        final var resultsMap =
            createResultsMap(expressionEngine.getVariableNames(), queryResults);

        final List<RequestError> errors = new ArrayList<RequestError>();

        final List<ShardedResultGroup> results = resultsMap.entrySet().stream()
            .filter(evaluateEntry -> {
                return filterBadSeries(evaluateEntry, expressionEngine.getVariableNames(), errors);
            })
            .map(evaluateEntry -> {
                return runArithmeticForSeries(evaluateEntry.getValue(), expressionEngine);
            }).collect(Collectors.toList());

        return new QueryMetricsResponse(
            newUUID,
            queryResults.values().stream().findFirst().get().getRange(),
            results,
            errors,
            QueryTrace.PASSIVE,
            ResultLimits.of(),
            Optional.empty(),
            Optional.empty()
        );
    }

}
