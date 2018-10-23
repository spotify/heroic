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
import net.objecthunter.exp4j.tokenizer.UnknownFunctionOrVariableException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

public class ArithmeticEngine {

  private static Optional<Expression> getExpressionEngine(final Arithmetic arithmetic,
                                                          final Map<String, QueryMetricsResponse> resultsMap) {
    try {
      final Expression expressionEngine = new ExpressionBuilder(arithmetic.getExpression())
          .variables(resultsMap.keySet())
          .build();
      return Optional.of(expressionEngine);
    } catch (UnknownFunctionOrVariableException e1) {
      return Optional.empty();
    }
  }

  public static QueryMetricsResponse run(final Arithmetic arithmetic,
                                         final Map<String, QueryMetricsResponse>
                                             resultsMap) {
    final UUID newUUID = UUID.randomUUID();
    final Optional<Expression> expressionEngineOptional =
        getExpressionEngine(arithmetic, resultsMap);
    if (expressionEngineOptional.isPresent()) {
      return evaluateExpression(newUUID, expressionEngineOptional.get(), resultsMap);
    } else {
      return new QueryMetricsResponse(
          newUUID,
          DateRange.create(0L, 1L),
          ImmutableList.of(),
          ImmutableList.of(new QueryError(String.format("Expression `%s` is invalid.", arithmetic
              .getExpression()))),
          QueryTrace.PASSIVE,
          ResultLimits.of(),
          Optional.empty(),
          Optional.empty()
      );
    }
  }

  private static Map<String, List<Pair<String, ShardedResultGroup>>> groupResultsByKey(
      final Set<String> variableNames,
      final Map<String, QueryMetricsResponse> resultsMap
  ) {
    Map<String, List<Pair<String, ShardedResultGroup>>> resultsToEvaluate = new HashMap<>();
    variableNames
        .forEach(name -> {
          final List<ShardedResultGroup> shardedResultGroups = resultsMap.get(name).getResult();
          shardedResultGroups
              .forEach(shardedResultGroup -> {
                final String key = shardedResultGroup.getKey().toString();
                if (!resultsToEvaluate.containsKey(key)) {
                  resultsToEvaluate.put(key, new ArrayList<Pair<String, ShardedResultGroup>>());
                }
                resultsToEvaluate.get(key).add(Pair.of(name, shardedResultGroup));
              });
        });
    return resultsToEvaluate;
  }

  private static boolean checkSeriesCountMatches(
      final Map.Entry<String, List<Pair<String, ShardedResultGroup>>> evaluateEntry,
      final Set<String> variableNames,
      final List<RequestError> errors) {
    if (evaluateEntry.getValue().size() == variableNames.size() && evaluateEntry.getValue().size
        () > 0) {
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
    final ShardedResultGroup sampleShardGroup = seriesEntries.get(0).getRight();
    final List<Point> sampleShardGroupPoints = sampleShardGroup.getMetrics().getDataAs(Point.class);
    final List<Point> resultsPoints = IntStream.range(0, sampleShardGroupPoints.size())
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
      final UUID newUUID,
      final Expression expressionEngine,
      final Map<String, QueryMetricsResponse> resultsMap) {

    final Map<String, List<Pair<String, ShardedResultGroup>>> resultsToEvaluate =
        groupResultsByKey(expressionEngine.getVariableNames(), resultsMap);
    final List<RequestError> errors = new ArrayList<RequestError>();
    final List<ShardedResultGroup> evaluateResults = resultsToEvaluate.entrySet().stream()
        .filter(evaluateEntry -> {
          return filterBadSeries(evaluateEntry, expressionEngine.getVariableNames(), errors);
        })
        .map(evaluateEntry -> {
          return runArithmeticForSeries(evaluateEntry.getValue(), expressionEngine);
        }).collect(Collectors.toList());

    return new QueryMetricsResponse(
        newUUID,
        resultsMap.values().stream().findFirst().get().getRange(),
        evaluateResults,
        errors,
        QueryTrace.PASSIVE,
        ResultLimits.of(),
        Optional.empty(),
        Optional.empty()
    );
  }

}
