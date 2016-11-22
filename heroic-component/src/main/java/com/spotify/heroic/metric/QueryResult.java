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

package com.spotify.heroic.metric;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.QueryRequestMetadata;
import com.spotify.heroic.aggregation.AggregationCombiner;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.OptionalLimit;
import eu.toolchain.async.Collector;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
@Data
public class QueryResult {
    private static Logger queryLog = LoggerFactory.getLogger("query.log");

    /**
     * The range in which all result groups metric's should be contained in.
     */
    private final DateRange range;

    /**
     * Groups of results.
     * <p>
     * Failed groups are omitted from here, {@link #errors} for these.
     */
    private final List<ShardedResultGroup> groups;

    /**
     * Errors that happened during the query.
     */
    private final List<RequestError> errors;

    /**
     * Query trace, if available.
     */
    private final QueryTrace trace;

    private final ResultLimits limits;

    private static LongAdder totalQueriesProcessed = new LongAdder();

    // Use AtomicLong since every time we do this we'll also read the value, so LongAdder is no use
    private static AtomicLong queriesAboveThreshold = new AtomicLong();

    /**
     * Collect result parts into a complete result.
     *
     * @param range The range which the result represents.
     * @return A complete QueryResult.
     */
    public static Collector<QueryResultPart, QueryResult> collectParts(
        final QueryTrace.Identifier what, final DateRange range, final AggregationCombiner combiner,
        final OptionalLimit groupLimit,
        final FullQuery.Request request,
        final QueryRequestMetadata requestMetadata,
        final boolean logQueries,
        final OptionalLimit logQueriesThresholdDataPoints

    ) {
        final QueryTrace.NamedWatch w = QueryTrace.watch(what);

        return parts -> {
            final List<List<ShardedResultGroup>> all = new ArrayList<>();
            final List<RequestError> errors = new ArrayList<>();
            final ImmutableList.Builder<QueryTrace> queryTraces = ImmutableList.builder();
            final ImmutableSet.Builder<ResultLimit> limits = ImmutableSet.builder();

            for (final QueryResultPart part : parts) {
                errors.addAll(part.getErrors());
                queryTraces.add(part.getQueryTrace());
                limits.addAll(part.getLimits().getLimits());

                if (part.isEmpty()) {
                    continue;
                }

                all.add(part.getGroups());
            }

            final List<ShardedResultGroup> groups = combiner.combine(all);
            final QueryTrace trace = w.end(queryTraces.build());

            if (groupLimit.isGreaterOrEqual(groups.size())) {
                limits.add(ResultLimit.GROUP);
            }

            QueryResult queryResult = new QueryResult(range, groupLimit.limitList(groups), errors,
                                                      trace, new ResultLimits(limits.build()));

            queryResult.logQueryInfo(request, requestMetadata,
                                     logQueries, logQueriesThresholdDataPoints);

            return queryResult;
        };
    }

    public void logQueryInfo(final FullQuery.Request request,
                             final QueryRequestMetadata requestMetadata,
                             final boolean logQueries,
                             final OptionalLimit logQueriesThresholdDataPoints
    ) {
        log.info("QueryResult:logQueryInfo entering");
        if (!logQueries) {
            log.info("QueryResult:logQueryInfo won't log queries");
            return;
        }

        totalQueriesProcessed.increment();

        long totalDataPoints = 0;
        for (ShardedResultGroup g : groups) {
            totalDataPoints += g.getMetrics().getData().size();
        }

        if (totalDataPoints < logQueriesThresholdDataPoints.orElse(
                                    OptionalLimit.of(0)).asLong().get()) {
            log.info("QueryResult:logQueryInfo Won't log because of threshold");
            return;
        }

        long currQueriesAboveThreshold = queriesAboveThreshold.incrementAndGet();

        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        dateFormat.setTimeZone(tz);
        String currentTimeAsISO = dateFormat.format(new Date());

        boolean isIPv6 = requestMetadata.getRemoteAddr().indexOf(':') != -1;
        String json = "{" +
                      " \"@timestamp\": \"" + currentTimeAsISO + "\"," +
                      " \"@message\": {" +
                      " \"numQueriesAboveThreshold\": " + currQueriesAboveThreshold + "," +
                      " \"totalQueries\": " + totalQueriesProcessed + "," +
                      " \"numDataPoints\": " + totalDataPoints + "," +
                      " \"elapsed\": " + trace.getElapsed() + "," +
                      " \"total/s\": " + (trace.getElapsed() == 0 ?
                                          0 : (1000000 * totalDataPoints) / trace.getElapsed()) +
                      "," +
                      " \"trace-what\": \"" + trace.getWhat().toString() + "\"," +
                      " \"preAggregationSampleSize\": " + trace.getPreAggregationSampleSize() +
                      "," +
                      " \"numSeries\": " + trace.getNumSeries() + "," +
                      " \"fromIP\": \"" +
                      (isIPv6 ? "[" : "") + requestMetadata.getRemoteAddr() + (isIPv6 ? "]" : "") +
                      ":" + requestMetadata.getRemotePort() + "\"" + "," +
                      " \"fromHost\": \"" + requestMetadata.getRemoteHost() + "\"," +
                      " \"user-agent\": \"" + requestMetadata.getRemoteUserAgent() + "\"," +
                      " \"client-id\": \"" + requestMetadata.getRemoteClientId() + "\"," +
                      " \"query\": \"" + escapeForJSON(request.toString()) + "\"," +
                      " \"children\": [";

        json += jsonForQueryTraceChildren(trace.getChildren());

        json += "]";
        json += "}";
        json += "}";

        queryLog.info(json);
    }

    public String jsonForQueryTraceChildren(final List<QueryTrace> children) {

        String out = new String();
        Iterator<QueryTrace> iterator = children.iterator();
        while (iterator.hasNext()) {
            QueryTrace child = iterator.next();
            out += "{" +
                   " \"what\": \"" + child.getWhat().toString() + "\"," +
                   " \"elapsed\": " + child.getElapsed() + "," +
                   " \"preAggregationSampleSize\": " + child.getPreAggregationSampleSize() +
                   "," +
                   " \"numSeries\": " + child.getNumSeries() + "," +
                   " \"children\": [";
            out += jsonForQueryTraceChildren(child.getChildren());
            out += "]";
            out += "}";
            out += (iterator.hasNext() ? ", " : "");
        }
        return out;
    }

    public String escapeForJSON(String s) {
        String out = new String();
        for (char c : s.toCharArray()) {
            if (c == '"') {
                out += "\\\"";
            } else if (c == '\\') {
                out += "\\\\";
            } else {
                out += c;
            }
        }
        return out;
    }
}
