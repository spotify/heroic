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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRawValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.metric.QueryResult;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.ShardedResultGroup;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.core.MediaType;

import lombok.AllArgsConstructor;
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
    private ObjectMapper objectMapper;

    private static LongAdder totalQueriesProcessed = new LongAdder();

    // Use AtomicLong since every time we do this we'll also read the value, so LongAdder is no use
    private static AtomicLong queriesAboveThreshold = new AtomicLong();

    @Inject
    public CoreQueryLogger(@Named ("logQueriesThresholdDataPoints") final OptionalLimit
        logQueriesThresholdDataPoints,
        @Named(MediaType.APPLICATION_JSON) final ObjectMapper objectMapper) {
        this.logQueriesThresholdDataPoints = logQueriesThresholdDataPoints;
        this.objectMapper = objectMapper;
    }

    public void logQueryAccess(Query query) {
        String idString;
        QueryOriginContext originContext;
        if (query != null && query.getOriginContext().isPresent()) {
            originContext = query.getOriginContext().get();
            idString = originContext.getQueryId().toString();
        } else {
            originContext = QueryOriginContext.empty();
            idString = "";
        }

        // Format timestamp nicely
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        dateFormat.setTimeZone(tz);
        String currentTimeAsISO = dateFormat.format(new Date());

        boolean isIPv6 = originContext.getRemoteAddr().indexOf(':') != -1;

        QueryAccessDataMessage message = new QueryAccessDataMessage(
            originContext.getQueryId(),
            (isIPv6 ? "[" : "") + originContext.getRemoteAddr() + (isIPv6 ? "]" : "") +
                ":" + originContext.getRemotePort(),
            originContext.getRemoteHost(),
            originContext.getRemoteUserAgent(),
            originContext.getRemoteClientId(),
            originContext.getQueryString());

        QueryAccessData queryAccessData = new QueryAccessData(currentTimeAsISO, message);

        String json;
        try {
            json = objectMapper.writeValueAsString(queryAccessData);
        } catch (JsonProcessingException e) {
            log.info("Failed to generate JSON for logging of query");
            return;
        }

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
            .orElse(QueryOriginContext.empty());

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

        // Format timestamp nicely
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

        QueryDoneMessageData message = new QueryDoneMessageData(
            status,
            (throwable == null ? null : throwable.toString()),
            originContext.getQueryId(),
            totalQueriesProcessed.longValue(),
            currQueriesAboveThreshold,
            postAggregationDataPoints,
            trace.getElapsed(),
            postAggregationDataPointsPerS,
            trace.getPreAggregationSampleSize(),
            trace.getNumSeries(),
            (isIPv6 ? "[" : "") + originContext.getRemoteAddr() + (isIPv6 ? "]" : "") +
                ":" + originContext.getRemotePort(),
            originContext.getRemoteHost(),
            originContext.getRemoteUserAgent(),
            originContext.getRemoteClientId(),
            originContext.getQueryString(),
            createQueryDoneChildList(trace.getChildren()));

        QueryDoneData queryDoneData = new QueryDoneData(currentTimeAsISO, message);

        String json;
        try {
            json = objectMapper.writeValueAsString(queryDoneData);
        } catch (JsonProcessingException e) {
            log.info("Failed to generate JSON for logging of query");
            return;
        }

        queryDoneLog.trace(json);
    }


    @AllArgsConstructor
    @Data
    class QueryAccessData {
        @JsonProperty("@timestamp")
        private String timestamp;
        @JsonProperty("@message")
        private QueryAccessDataMessage message;
    }
    @AllArgsConstructor
    @Data
    class QueryAccessDataMessage {
        private UUID uuid;
        private String fromIP;
        private String fromHost;
        private String userAgent;
        private String clientId;
        // The query String already contains the original JSON, so tell Jackson to not escape it
        @JsonRawValue
        private String query;
    }


    @AllArgsConstructor
    @Data
    class QueryDoneData {
        @JsonProperty("@timestamp")
        String timestamp;
        @JsonProperty("@message")
        QueryDoneMessageData message;
    }

    @AllArgsConstructor
    @Data
    class QueryDoneMessageData {
        String status;
        String error;
        UUID uuid;
        long totalQueries;
        long numQueriesAboveThreshold;
        long postAggregationDataPoints;
        long elapsed;
        long postAggregationDataPointsPerS;
        long preAggregationDataPoints;
        long preAggregationSeries;
        String fromIP;
        String fromHost;
        private String userAgent;
        private String clientId;
        // The query String already contains the original JSON, so tell Jackson to not escape it
        @JsonRawValue
        private String query;
        List<QueryDoneChildData> queryTraceChildren;
    }

    @AllArgsConstructor
    @Data
    class QueryDoneChildData {
        private String traceLevel;
        private long elapsed;
        private long preAggregationDataPoints;
        private long preAggregationSeries;
        List<QueryDoneChildData> queryTraceChildren;
    }

    private List<QueryDoneChildData> createQueryDoneChildList(List<QueryTrace> queryTraces) {
        List<QueryDoneChildData> list = new ArrayList<>();
        Iterator<QueryTrace> iterator = queryTraces.iterator();

        while (iterator.hasNext()) {
            QueryTrace queryTrace = iterator.next();
            list.add(createQueryDoneChild(queryTrace));
        }
        return list;
    }

    private QueryDoneChildData createQueryDoneChild(QueryTrace queryTrace) {
        List<QueryDoneChildData> children = createQueryDoneChildList(queryTrace.getChildren());

        QueryDoneChildData child = new QueryDoneChildData(queryTrace.getWhat().toString(),
            queryTrace.getElapsed(), queryTrace.getPreAggregationSampleSize(),
            queryTrace.getNumSeries(), children);

        return child;
    }

}
