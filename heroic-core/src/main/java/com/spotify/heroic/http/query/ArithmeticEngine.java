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

    package com.spotify.heroic.http.query;

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
    import java.util.AbstractMap;
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

    public class ArithmeticEngine {

        private static Optional<Expression> getExpression(final Arithmetic arithmetic,
            final Map<String, QueryMetricsResponse> resultsMap) {

            if (!arithmetic.getExpression().isPresent()) {
                return Optional.empty();
            }

            try {
                final var expression = new ExpressionBuilder(
                    arithmetic.getExpression().get())
                    .variables(resultsMap.keySet())
                    .build();

                return Optional.of(expression);
            } catch (IllegalArgumentException e) {
                // TODO report this somehow to caller
                return Optional.empty();
            }
        }

        public static QueryMetricsResponse run(final Arithmetic arithmetic,
            final Map<String, QueryMetricsResponse> queryResults) {
            final UUID newUUID = UUID.randomUUID();

            final Optional<Expression> expression =
                getExpression(arithmetic, queryResults);

            if (expression.isPresent()) {
                return evaluateExpression(newUUID, expression.get(), queryResults);
            } else {
                return new QueryMetricsResponse(
                    newUUID,
                    DateRange.create(0L, 1L),
                    ImmutableList.of(),
                    ImmutableList
                        .of(new QueryError(String.format("Expression `%s` is invalid.", arithmetic
                            .getExpression()))),
                    QueryTrace.PASSIVE,
                    ResultLimits.of(),
                    Optional.empty(),
                    Optional.empty()
                );
            }
        }

        private static Map<String, List<Pair<String, ShardedResultGroup>>> createResultsHolder(
            final Set<String> variableNames,
            final Map<String, QueryMetricsResponse> resultsMap) {

            Map<String, List<Pair<String, ShardedResultGroup>>> resultsHolder = new HashMap<>();

            variableNames
                .forEach(name -> {
                    final List<ShardedResultGroup> shardedResultGroups =
                        resultsMap.get(name).getResult();
                    shardedResultGroups
                        .forEach(shardedResultGroup -> {
                            final String key = shardedResultGroup.getKey().toString();

                            if (!resultsHolder.containsKey(key)) {
                                resultsHolder
                                    .put(key, new ArrayList<Pair<String, ShardedResultGroup>>());
                            }

                            resultsHolder.get(key).add(Pair.of(name, shardedResultGroup));
                        });
                });
            return resultsHolder;
        }

        private static boolean checkSeriesCountMatches(
            final Map.Entry<String, List<Pair<String, ShardedResultGroup>>> evaluateEntry,
            final Set<String> variableNames,
            final List<RequestError> errors) {
            if (evaluateEntry.getValue().size() == variableNames.size()
                && evaluateEntry.getValue().size
                () > 0) {
                return true;
            } else {
                final Set<String> variableSet = new HashSet<>(variableNames);
                variableSet.removeAll(
                    evaluateEntry.getValue().stream().map(Pair::getLeft)
                        .collect(Collectors.toSet()));
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
            final boolean seriesCountMatches = checkSeriesCountMatches(
                evaluateEntry,
                variableNames,
                errors);
            if (!seriesCountMatches) {
                return false;
            }
            final boolean seriesSizesMatch = checkSeriesSizes(evaluateEntry, errors);
            if (!seriesSizesMatch) {
                return false;
            }
            return checkSeriesTimestamps(evaluateEntry, errors);
        }

        private static Map<String, Double> getElementsAt(final int i,
            final List<Pair<String, ShardedResultGroup>> seriesEntries) {

            return seriesEntries.stream().map(entry -> {
                return new AbstractMap.SimpleEntry<>(entry.getLeft(),
                    entry.getRight()
                        .getMetrics()
                        .getDataAs(Point.class).get(i).getValue());
            })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        private static ShardedResultGroup runArithmeticForSeries(
            final List<Pair<String, ShardedResultGroup>> seriesEntries,
            final Expression expressionEngine) {

            final var sampleShardGroup = seriesEntries.get(0).getRight();

            final var sampleShardGroupPoints = sampleShardGroup.getMetrics().getDataAs(Point.class);

            final var resultsPoints =
                IntStream.range(0, sampleShardGroupPoints.size())
                    .mapToObj(i -> {
                        final Map<String, Double> variableMap = getElementsAt(i, seriesEntries);
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

            final var resultsHolder =
                createResultsHolder(expressionEngine.getVariableNames(), queryResults);

            final List<RequestError> errors = new ArrayList<RequestError>();

            final List<ShardedResultGroup> results = resultsHolder.entrySet().stream()
                .filter(evaluateEntry -> {
                    return filterBadSeries(evaluateEntry, expressionEngine.getVariableNames(),
                        errors);
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
