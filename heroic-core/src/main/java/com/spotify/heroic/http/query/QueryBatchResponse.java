package com.spotify.heroic.http.query;

import java.util.Map;

import lombok.Data;

@Data
public class QueryBatchResponse {
    final Map<String, QueryMetricsResponse> results;
}