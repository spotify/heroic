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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.Query;
import com.spotify.heroic.metric.FullQuery;
import com.spotify.heroic.metric.FullQuery.Request;
import com.spotify.heroic.metric.QueryMetrics;
import com.spotify.heroic.metric.QueryMetricsResponse;
import com.spotify.heroic.querylogging.format.LogFormat;
import com.spotify.heroic.querylogging.format.MessageFormat;
import java.time.Instant;
import java.util.function.Consumer;
import org.slf4j.Logger;

@QueryLoggingScope
public class Slf4jQueryLogger implements QueryLogger {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(Slf4jQueryLogger.class);
    private final Consumer<String> queryLog;
    private final ObjectMapper objectMapper;
    private final String component;

    @java.beans.ConstructorProperties({ "queryLog", "objectMapper", "component" })
    public Slf4jQueryLogger(final Consumer<String> queryLog, final ObjectMapper objectMapper,
                            final String component) {
        this.queryLog = queryLog;
        this.objectMapper = objectMapper;
        this.component = component;
    }

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
        logRequest(context, request, "outgoing-request-to-shards");
    }

    @Override
    public void logIncomingRequestAtNode(
        final QueryContext context, final FullQuery.Request request
    ) {
        logRequest(context, request, "incoming-request-at-node");
    }

    @Override
    public void logBigtableQueryTimeout(
        final QueryContext context, final FullQuery.Request request
    ) {
        logRequest(context, request, "bigtable-query-timeout");
    }

    @Override
    public void logOutgoingResponseAtNode(final QueryContext context, final FullQuery response) {
        logQuery(context, response, "outgoing-response-at-node");
    }


    @Override
    public void logIncomingResponseFromShard(
        final QueryContext context, final FullQuery response
    ) {
        logQuery(context, response, "incoming-response-from-shard");
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

    private void logQuery(QueryContext context, FullQuery response, String s) {
        performAndCatch(() -> {
            final FullQuery.Summary summary = response.summarize();
            serializeAndLog(context, s, summary);
        });
    }

    private void logRequest(QueryContext context, Request request, String s) {
        performAndCatch(() -> {
            final Request.Summary summary = request.summarize();
            serializeAndLog(context, s, summary);
        });
    }

    private <T> void serializeAndLog(
        final QueryContext context, final String type, final T data
    ) {
        performAndCatch(() -> {
            final MessageFormat<T> message =
                new MessageFormat<>(component, context.queryId(), context.clientContext(),
                    context.httpContext(), type, data);

            final String timestamp = Instant.now().toString();
            final LogFormat<T> logFormat = new LogFormat<>(timestamp, message);
            try {
                queryLog.accept(objectMapper.writeValueAsString(logFormat));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * PSK: I have serious reservations about this approach. It _appears_ to try to
     * spin up a new thread per log message, which in theory could prevent the calling thread
     * from blocking and speed processing up overall. However it would create and destroy
     * a *lot* of Thread objects and hardware/OS-level threads and potentially slow processing
     * down overall.
     *
     * However these thoughts are moot - notice that no Thread object is created and no
     * `start()` method is called, which means that all callers of performAndCatch write the log
     * message _in the calling thread_, thus defeating the whole point of performAndCatch it would
     * seem - to me at least.
     *
     * @param toRun Runnable to run in the calling thread
     */
    private static void performAndCatch(Runnable toRun) {
        try {
            toRun.run();
        } catch (Exception e) {
            log.error("Failed while trying to log query", e);
        }
    }
}
