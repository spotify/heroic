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

import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.Query;
import com.spotify.heroic.QueryManager;
import com.spotify.heroic.common.JavaxRestFramework;
import com.spotify.heroic.http.CoreHttpContextFactory;
import com.spotify.heroic.http.arithmetic.SeriesArithmeticOperator;
import com.spotify.heroic.metric.QueryMetrics;
import com.spotify.heroic.metric.QueryMetricsResponse;
import com.spotify.heroic.metric.QueryResult;
import com.spotify.heroic.querylogging.HttpContext;
import com.spotify.heroic.querylogging.QueryContext;
import com.spotify.heroic.querylogging.QueryLogger;
import com.spotify.heroic.querylogging.QueryLoggerFactory;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.tuple.Triple;
import org.jetbrains.annotations.NotNull;

@Path("query")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class QueryResource {

    public static final int RESPONSE_TIMEOUT_SECS = 300;
    private final JavaxRestFramework httpAsync;
    private final QueryManager manager;
    private final AsyncFramework async;
    private final QueryLogger queryLogger;
    private static final Tracer tracer = Tracing.getTracer();


    @Inject
    public QueryResource(
        final JavaxRestFramework httpAsync,
        final QueryManager query,
        final AsyncFramework async,
        final QueryLoggerFactory queryLoggerFactory
    ) {
        this.httpAsync = httpAsync;
        this.manager = query;
        this.async = async;
        this.queryLogger = queryLoggerFactory.create("QueryResource");
    }

    @POST
    @Path("metrics")
    @Consumes(MediaType.TEXT_PLAIN)
    public void metricsText(
        @Suspended final AsyncResponse response, @QueryParam("group") String group,
        @Context final HttpServletRequest servletReq, final String query
    ) {
        final HttpContext httpContext = CoreHttpContextFactory.create(servletReq);
        final QueryContext queryContext = QueryContext.create(Optional.empty(), httpContext);
        queryLogger.logHttpQueryText(queryContext, query);

        final Query q = this.manager.newQueryFromString(query).build();

        final QueryManager.Group g = this.manager.useOptionalGroup(Optional.ofNullable(group));
        final AsyncFuture<QueryResult> callback = g.query(q, queryContext);

        bindMetricsResponse(response, callback, queryContext);
    }

    @POST
    @Path("metrics")
    @Consumes(MediaType.APPLICATION_JSON)
    public void metrics(
        @Suspended final AsyncResponse response,
        @QueryParam("group") String group,
        @Context final HttpServletRequest servletReq,
        final QueryMetrics query
    ) {
        final HttpContext httpContext = CoreHttpContextFactory.create(servletReq);
        final QueryContext queryContext = QueryContext.create(query.clientContext(), httpContext);
        queryLogger.logHttpQueryJson(queryContext, query);

        final Query q = query.toQueryBuilder(this.manager::newQueryFromString).build();

        final QueryManager.Group g = this.manager.useOptionalGroup(Optional.ofNullable(group));
        final AsyncFuture<QueryResult> callback = g.query(q, queryContext);

        bindMetricsResponse(response, callback, queryContext);
    }

    @POST
    @Path("batch")
    public void metrics(
        @Suspended final AsyncResponse response,
        @QueryParam("backend") String group,
        @Context final HttpServletRequest servletReq,
        final QueryBatch query
    ) {
        final var batch = new QueryBatch(query.getQueries(), query.getRange());

        // Create the future work of querying for the metrics
        final var queryBatchResponseFuture =
            createQueryBatchResponseCompositeFuture(response, group, servletReq, batch);

        // Add to that work, the work of transforming the resulting metrics with the arithmetic
        // operation(s).
        final var arithmeticFuture =
            augmentFuturesWithArithmeticFutures(query, queryBatchResponseFuture);

        response.setTimeout(RESPONSE_TIMEOUT_SECS, TimeUnit.SECONDS);

        // wire-up the response to the arithmetic future, which will make this call wait for the
        // future (and the futures from which it is composed) to resolve completely.
        httpAsync.bind(response, arithmeticFuture);
    }

    private static AsyncFuture<QueryBatchResponse> augmentFuturesWithArithmeticFutures(
        QueryBatch query, AsyncFuture<QueryBatchResponse> queryBatchResponseFuture) {

        var future = queryBatchResponseFuture;

        final var arithmeticQueries = query.getArithmeticQueries();

        if (arithmeticQueries.isEmpty()) {
            return future;
        }

        final ImmutableMap.Builder<String, QueryMetricsResponse> arithmeticResults =
            ImmutableMap.builder();

        // transform the batch response
        future = queryBatchResponseFuture.directTransform(batchResponse -> {

            // 1. get the batch response's results e.g. {"A":{<results>},"B":{<results>},...}
            final var responses = batchResponse.getResults();

            // Apply each arithmetic operation to each result
            arithmeticQueries.get().forEach((queryName, arithmetic) -> {

                // Here's where we actually perform the arithmetic operation
                // TODO convert this to dagger e.g.
//                var op = DaggerSeriesArithmeticOperatorComponent.create();
                var operator = new SeriesArithmeticOperator(arithmetic, responses);
                var reducedResponse = operator.reduce();
                arithmeticResults.put(queryName, reducedResponse);
            });

            arithmeticResults.putAll(batchResponse.getResults());

            return new QueryBatchResponse(arithmeticResults.build());
        });

        return future;
    }

    private AsyncFuture<QueryBatchResponse> createQueryBatchResponseCompositeFuture(
        @Suspended final AsyncResponse response,
        @QueryParam("backend") String group,
        @Context final HttpServletRequest servletReq,
        final QueryBatch query
    ) {
        final HttpContext httpContext = CoreHttpContextFactory.create(servletReq);
        final QueryManager.Group queryManagerGroup =
            this.manager.useOptionalGroup(Optional.ofNullable(group));

        final var futures = createQueryFutures(query, httpContext, queryManagerGroup);

        // turn the result of each AsyncFuture<Triple<String, QueryContext, QueryResult>>
        // entry into a single AsyncFuture<QueryBatchResponse>.
        return async.collect(futures).directTransform(entries -> {
            final ImmutableMap.Builder<String, QueryMetricsResponse> results =
                ImmutableMap.builder();

            for (final var triple : entries) {
                final String queryKey = triple.getLeft();
                final QueryContext queryContext = triple.getMiddle();
                final QueryResult r = triple.getRight();

                final QueryMetricsResponse qmr =
                    new QueryMetricsResponse(queryContext.queryId(), r.getRange(),
                        r.getGroups(), r.getErrors(), r.getTrace(), r.getLimits(),
                        Optional.of(r.getPreAggregationSampleSize()), r.getCache());

                queryLogger.logFinalResponse(queryContext, qmr);

                results.put(queryKey, qmr);
            }

            return new QueryBatchResponse(results.build());
        });
    }

    /**
     * Create a list of in-flight {@link QueryManager.Group#query(Query, QueryContext)} calls,
     * wrapped in futures. Then transform the QueryResult into a triple, which connects the result
     * to the query that produced it.
     *
     * @param batchQuery        a batch of query definitions to execute
     * @param httpContext       used for query logging
     * @param queryManagerGroup the vehicle for performing the query
     * @return triple which connects the result to the query that produced it
     */
    @NotNull
    private List<AsyncFuture<Triple<String, QueryContext, QueryResult>>> createQueryFutures(
        QueryBatch batchQuery, HttpContext httpContext, QueryManager.Group queryManagerGroup) {

        /*
         A map of Query names to query definitions e.g.
         { 'A' → {query:x,aggregation:y,source:z,range:p},
           'B' → {query:a,aggregation:b,source:c,range:d} }
         */
        final Map<String, QueryMetrics> queries =
            batchQuery.getQueries().orElse(new HashMap<String, QueryMetrics>());

        final List<AsyncFuture<Triple<String, QueryContext, QueryResult>>> futures =
            new ArrayList<>(queries.size());

        final Span currentSpan = tracer.getCurrentSpan();

        for (final var e : queries.entrySet()) {

            final String queryName = e.getKey();
            final QueryMetrics query = e.getValue();

            final Query q = query
                .toQueryBuilder(this.manager::newQueryFromString)
                .rangeIfAbsent(batchQuery.getRange())
                .build();

            final var queryContext =
                QueryContext.create(query.clientContext(), httpContext);
            queryLogger.logHttpQueryJson(queryContext, query);

            // Here is the call to query which sprays out queries to shards and collects
            // the results i.e. doesn't wait for them but presents their suspended processing
            // (or the already-received result) as a single future.
            futures.add(queryManagerGroup
                .query(q, queryContext, currentSpan)
                .directTransform(r -> Triple.of(queryName, queryContext, r)));
        }

        return futures;
    }

    private void bindMetricsResponse(
        final AsyncResponse response,
        final AsyncFuture<QueryResult> callback,
        final QueryContext queryContext
    ) {
        response.setTimeout(RESPONSE_TIMEOUT_SECS, TimeUnit.SECONDS);

        httpAsync.bind(response, callback, r -> {
            final QueryMetricsResponse qmr =
                new QueryMetricsResponse(queryContext.queryId(), r.getRange(), r.getGroups(),
                    r.getErrors(), r.getTrace(), r.getLimits(),
                    Optional.of(r.getPreAggregationSampleSize()), r.getCache());
            queryLogger.logFinalResponse(queryContext, qmr);

            return qmr;
        });
    }
}
