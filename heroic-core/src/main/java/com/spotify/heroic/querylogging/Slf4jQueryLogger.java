/*
 * Copyright (c) 2017 Spotify AB.
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

package com.spotify.heroic.querylogging;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.Query;
import com.spotify.heroic.metric.FullQuery;
import com.spotify.heroic.metric.QueryMetrics;
import com.spotify.heroic.metric.QueryMetricsResponse;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@QueryLoggingScope
@Slf4j
@RequiredArgsConstructor
public class Slf4jQueryLogger implements QueryLogger {
    private final Consumer<String> queryLog;
    private final ObjectMapper objectMapper;
    private final String component;

    @Override
    public void logHttpQueryText(
        final QueryContext context, final String query
    ) {
        serializeAndLog(context, "http-query-text", query);
    }

    @Override
    public void logHttpQueryJson(
        final QueryContext context, final QueryMetrics query
    ) {
        serializeAndLog(context, "http-query-json", query);
    }

    @Override
    public void logQuery(final QueryContext context, final Query query) {
        serializeAndLog(context, "query", query);
    }

    @Override
    public void logOutgoingRequestToShards(
        final QueryContext context, final FullQuery.Request request
    ) {
        performAndCatch(() -> {
            final FullQuery.Request.Summary summary = request.summarize();
            serializeAndLog(context, "outgoing-request-to-shards", summary);
        });
    }

    @Override
    public void logIncomingRequestAtNode(
        final QueryContext context, final FullQuery.Request request
    ) {
        performAndCatch(() -> {
            final FullQuery.Request.Summary summary = request.summarize();
            serializeAndLog(context, "incoming-request-at-node", summary);
        });
    }

    @Override
    public void logOutgoingResponseAtNode(final QueryContext context, final FullQuery response) {
        performAndCatch(() -> {
            final FullQuery.Summary summary = response.summarize();
            serializeAndLog(context, "outgoing-response-at-node", summary);
        });
    }

    @Override
    public void logIncomingResponseFromShard(
        final QueryContext context, final FullQuery response
    ) {
        performAndCatch(() -> {
            final FullQuery.Summary summary = response.summarize();
            serializeAndLog(context, "incoming-response-from-shard", summary);
        });
    }

    @Override
    public void logFinalResponse(
        final QueryContext context, final QueryMetricsResponse queryMetricsResponse
    ) {
        performAndCatch(() -> {
            final QueryMetricsResponse.Summary summary = queryMetricsResponse.summarize();
            serializeAndLog(context, "final-response", summary);
        });
    }

    private <T> void serializeAndLog(
        final QueryContext context, final String type, final T data
    ) {
        performAndCatch(() -> {
            final MessageFormat<T> message =
                new MessageFormat<>(component, context.getQueryId(), context.getClientContext(),
                    context.getHttpContext(), type, data);

            final String timestamp = Instant.now().toString();
            final LogFormat logFormat = new LogFormat(timestamp, message);
            try {
                queryLog.accept(objectMapper.writeValueAsString(logFormat));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void performAndCatch(Runnable toRun) {
        try {
            toRun.run();
        } catch (Exception e) {
            log.error("Failed while trying to log query", e);
        }
    }

    @Data
    public class LogFormat {
        @JsonProperty("@timestamp")
        private final String timestamp;
        @JsonProperty("@message")
        private final MessageFormat message;
    }

    @Data
    public class MessageFormat<T> {
        private final String component;
        private final UUID queryId;
        private final Optional<JsonNode> clientContext;
        private final Optional<HttpContext> httpContext;
        private final String type;
        private final T data;
    }
}
