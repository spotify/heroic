package com.spotify.heroic.http.query;

import com.spotify.heroic.QueryDateRange;
import com.spotify.heroic.metric.Arithmetic;
import com.spotify.heroic.metric.QueryMetrics;
import java.util.Map;
import java.util.Optional;
import lombok.Data;

@Data
public class QueryBatchV2 {
  final Optional<Map<String, QueryMetrics>> queries;
  final Optional<QueryDateRange> range;
  final Optional<Map<String, Arithmetic>> arithmeticQueries;
}

