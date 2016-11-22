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


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.Query;
import com.spotify.heroic.QueryManager;
import com.spotify.heroic.QueryOriginContext;
import com.spotify.heroic.common.JavaxRestFramework;
import com.spotify.heroic.metric.QueryResult;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import javax.inject.Named;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("query")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Slf4j
public class QueryResource {
    private static Logger queryAccessLog = LoggerFactory.getLogger("query.access.log");
    private final JavaxRestFramework httpAsync;
    private final QueryManager query;
    private final AsyncFramework async;
    private final ObjectMapper objectMapper;

    @Inject
    public QueryResource(JavaxRestFramework httpAsync, QueryManager query, AsyncFramework async,
                         @Named(MediaType.APPLICATION_JSON) final ObjectMapper mapper) {
        this.httpAsync = httpAsync;
        this.query = query;
        this.async = async;
        this.objectMapper = mapper;
    }

    @POST
    @Path("metrics")
    @Consumes(MediaType.TEXT_PLAIN)
    public void metricsText(
        @Suspended final AsyncResponse response, @QueryParam("group") String group,
        @Context HttpServletRequest servletReq, String query
    ) {
        final Query q = this.query.newQueryFromString(query)
            .queryRequestMetadata(Optional.of(
                CoreQueryOriginContextFactory.create(servletReq, query)))
            .build();

        logQueryAccess(query, q);

        final QueryManager.Group g = this.query.useOptionalGroup(Optional.ofNullable(group));
        final AsyncFuture<QueryResult> callback = g.query(q);

        bindMetricsResponse(response, callback);
    }

    @POST
    @Path("metrics")
    @Consumes(MediaType.APPLICATION_JSON)
    public void metricsJson(
        @Suspended final AsyncResponse response, @QueryParam("group") String group,
        @Context HttpServletRequest servletReq, String queryString
    ) {
        QueryMetrics query = parseQueryMetricsFrom(queryString);
        if (query == null) {
            response.cancel();
            return;
        }

        final Query q = query.toQueryBuilder(this.query::newQueryFromString)
            .queryRequestMetadata(Optional.of(
                CoreQueryOriginContextFactory.create(servletReq, queryString)))
            .build();

        logQueryAccess(queryString, q);

        final QueryManager.Group g = this.query.useOptionalGroup(Optional.ofNullable(group));
        final AsyncFuture<QueryResult> callback = g.query(q);

        bindMetricsResponse(response, callback);
    }

    @POST
    @Path("batch")
    public void metricsBatch(
        @Suspended final AsyncResponse response, @QueryParam("backend") String group,
        @Context HttpServletRequest servletReq, final String queryString
    ) {
        QueryBatch query = parseQueryBatchFrom(queryString);
        if (query == null) {
            response.cancel();
            return;
        }

        logQueryAccess(queryString, null);

        final QueryManager.Group g = this.query.useOptionalGroup(Optional.ofNullable(group));

        final List<AsyncFuture<Pair<String, QueryResult>>> futures = new ArrayList<>();

        query.getQueries().ifPresent(queries -> {
            for (final Map.Entry<String, QueryMetrics> e : queries.entrySet()) {
                final Query q = e
                    .getValue()
                    .toQueryBuilder(this.query::newQueryFromString)
                    .rangeIfAbsent(query.getRange())
                    .queryRequestMetadata(Optional.of(
                        CoreQueryOriginContextFactory.create(servletReq, queryString)))
                    .build();

                futures.add(g.query(q).directTransform(r -> Pair.of(e.getKey(), r)));
            }
        });

        final AsyncFuture<QueryBatchResponse> future =
            async.collect(futures).directTransform(entries -> {
                final ImmutableMap.Builder<String, QueryMetricsResponse> results =
                    ImmutableMap.builder();

                for (final Pair<String, QueryResult> e : entries) {
                    final QueryResult r = e.getRight();
                    results.put(e.getLeft(),
                        new QueryMetricsResponse(r.getRange(), r.getGroups(), r.getErrors(),
                            r.getTrace(), r.getLimits()));
                }

                return new QueryBatchResponse(results.build());
            });

        response.setTimeout(300, TimeUnit.SECONDS);

        httpAsync.bind(response, future);
    }

    private void bindMetricsResponse(
        final AsyncResponse response, final AsyncFuture<QueryResult> callback
    ) {
        response.setTimeout(300, TimeUnit.SECONDS);

        httpAsync.bind(response, callback,
            r -> new QueryMetricsResponse(r.getRange(), r.getGroups(), r.getErrors(), r.getTrace(),
                r.getLimits()));
    }

    private void logQueryAccess(String query, Query q) {
        String idString;
        QueryOriginContext originContext;
        if (q != null && q.getOriginContext().isPresent()) {
            originContext = q.getOriginContext().get();
            idString = originContext.getQueryId().toString();
        } else {
            originContext = QueryOriginContext.of();
            idString = "";
        }

        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        dateFormat.setTimeZone(tz);
        String currentTimeAsISO = dateFormat.format(new Date());

        boolean isIPv6 = originContext.getRemoteAddr().indexOf(':') != -1;
        String json = "{" +
                      " \"@timestamp\": \"" + currentTimeAsISO + "\"," +
                      " \"@message\": {" +
                      " \"UUID\": \"" + originContext.getQueryId() + "\"," +
                      " \"fromIP\": \"" +
                      (isIPv6 ? "[" : "") + originContext.getRemoteAddr() + (isIPv6 ? "]" : "") +
                      ":" + originContext.getRemotePort() + "\"" + "," +
                      " \"fromHost\": \"" + originContext.getRemoteHost() + "\"," +
                      " \"user-agent\": \"" + originContext.getRemoteUserAgent() + "\"," +
                      " \"client-id\": \"" + originContext.getRemoteClientId() + "\"," +
                      " \"query\": " + originContext.getQueryString() +
                      "}" +
                      "}";

        queryAccessLog.trace(json);
    }

    private QueryMetrics parseQueryMetricsFrom(String queryString) {
        QueryMetrics query = null;
        try {
            query = objectMapper.readValue(queryString, QueryMetrics.class);
        } catch (IOException e) {
            log.info("Failed to deserialize JSON to QueryMetrics object! \"" + queryString + "\"");
            log.info("Error: " + e.toString());
        }
        return query;
    }

    private QueryBatch parseQueryBatchFrom(String queryString) {
        QueryBatch query = null;
        try {
            query = objectMapper.readValue(queryString, QueryBatch.class);
        } catch (IOException e) {
            log.info("Failed to deserialize JSON to QueryBatch object! \"" + queryString + "\"");
            log.info("Error: " + e.toString());
        }
        return query;
    }

    @Data
    public static final class StreamId {
        private final Map<String, String> tags;
        private final UUID id;
    }

    @Data
    private static final class StreamQuery {
        private final QueryManager.Group group;
        private final Query query;
    }
}
