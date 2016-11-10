/*
 * Copyright (c) 2016 Spotify AB.
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

package com.spotify.heroic;

import com.spotify.heroic.Query;
import com.spotify.heroic.QueryLogger;
import com.spotify.heroic.QueryOriginContext;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.metric.QueryResult;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.ShardedResultGroup;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.Data;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
@Data
public class CoreQueryLogger implements QueryLogger {
    private static Logger queryAccessLog = LoggerFactory.getLogger("query.access.log");
    private static Logger queryDoneLog = LoggerFactory.getLogger("query.done.log");

    private OptionalLimit logQueriesThresholdDataPoints;

    private static LongAdder totalQueriesProcessed = new LongAdder();

    // Use AtomicLong since every time we do this we'll also read the value, so LongAdder is no use
    private static AtomicLong queriesAboveThreshold = new AtomicLong();

    @Inject
    public CoreQueryLogger(@Named ("logQueriesThresholdDataPoints") final OptionalLimit
                               logQueriesThresholdDataPoints) {
        this.logQueriesThresholdDataPoints = logQueriesThresholdDataPoints;
    }

    public void logQueryAccess(Query query) {
        String idString;
        QueryOriginContext originContext;
        if (query != null && query.getOriginContext().isPresent()) {
            originContext = query.getOriginContext().get();
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

    public void logQueryFailed(Query query, Throwable t) {
        logQueryDone(query, null, "failed", t);
    }
    public void logQueryResolved(Query query, QueryResult queryResult) {
        logQueryDone(query, queryResult, "resolved", null);
    }
    public void logQueryCancelled(Query query) {
        logQueryDone(query, null, "cancelled", null);
    }

    public void logQueryDone(Query query, QueryResult result, String status, Throwable throwable) {
        final QueryTrace trace = result.getTrace();
        final List<ShardedResultGroup> groups = result.getGroups();
        final QueryOriginContext originContext = query.getOriginContext()
            .orElse(QueryOriginContext.of());

        log.info("QueryResult:logQueryDone entering");

        totalQueriesProcessed.increment();

        int postAggregationDataPoints = 0;
        for (ShardedResultGroup g : groups) {
            postAggregationDataPoints += g.getMetrics().getData().size();
        }

        if (!logQueriesThresholdDataPoints.isGreaterOrEqual(postAggregationDataPoints)) {
            log.info("QueryResult:logQueryDone Won't log because of threshold");
            return;
        }

        long currQueriesAboveThreshold = queriesAboveThreshold.incrementAndGet();

        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        dateFormat.setTimeZone(tz);
        String currentTimeAsISO = dateFormat.format(new Date());

        boolean isIPv6 = originContext.getRemoteAddr().indexOf(':') != -1;
        long postAggregationDataPointsPerS = 0;
        if (trace.getElapsed() != 0) {
            postAggregationDataPointsPerS =
                (1000000 * postAggregationDataPoints) / trace.getElapsed();
        }
        String json = "{" +
            " \"@timestamp\": \"" + currentTimeAsISO + "\"," +
            " \"status\": \"" + status + "\"," +
            (throwable == null ? "" : " \"error\": \"" + throwable.toString() + "\"") +
            " \"@message\": {" +
            " \"UUID\": \"" + originContext.getQueryId() + "\"," +
            " \"numQueriesAboveThreshold\": " + currQueriesAboveThreshold + "," +
            " \"totalQueries\": " + totalQueriesProcessed + "," +
            " \"postAggregationDataPoints\": " + postAggregationDataPoints + "," +
            " \"elapsed\": " + trace.getElapsed() + "," +
            " \"postAggregationDataPoints/s\": " + postAggregationDataPointsPerS + "," +
            " \"preAggregationSampleSize\": " + trace.getPreAggregationSampleSize() + "," +
            " \"numSeries\": " + trace.getNumSeries() + "," +
            " \"trace-what\": \"" + trace.getWhat().toString() + "\"," +
            " \"fromIP\": \"" +
            (isIPv6 ? "[" : "") + originContext.getRemoteAddr() + (isIPv6 ? "]" : "") +
            ":" + originContext.getRemotePort() + "\"" + "," +
            " \"fromHost\": \"" + originContext.getRemoteHost() + "\"," +
            " \"user-agent\": \"" + originContext.getRemoteUserAgent() + "\"," +
            " \"client-id\": \"" + originContext.getRemoteClientId() + "\"," +
            " \"query\": " + originContext.getQueryString() + "," +
            " \"children\": [";

        json += jsonForQueryTraceChildren(trace.getChildren());

        json += "]";
        json += "}";
        json += "}";

        queryDoneLog.trace(json);
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
}
