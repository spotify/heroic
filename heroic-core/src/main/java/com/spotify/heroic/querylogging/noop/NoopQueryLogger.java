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

package com.spotify.heroic.querylogging.noop;

import com.spotify.heroic.Query;
import com.spotify.heroic.metric.FullQuery;
import com.spotify.heroic.metric.QueryMetrics;
import com.spotify.heroic.metric.QueryMetricsResponse;
import com.spotify.heroic.querylogging.HttpContext;
import com.spotify.heroic.querylogging.QueryContext;
import com.spotify.heroic.querylogging.QueryLogger;
import com.spotify.heroic.querylogging.QueryLoggingScope;
import lombok.extern.slf4j.Slf4j;

@QueryLoggingScope
@Slf4j
public class NoopQueryLogger implements QueryLogger {
    @Override
    public void logHttpQueryText(
        final QueryContext context, final String query
    ) {
    }

    @Override
    public void logHttpQueryJson(
        final QueryContext context, final QueryMetrics query
    ) {
    }

    public void logHttpQueryObject(
        final QueryContext context, final Object query, final HttpContext httpContext
    ) {
    }

    @Override
    public void logQuery(final QueryContext context, final Query query) {
    }

    @Override
    public void logOutgoingRequestToShards(
        final QueryContext context, final FullQuery.Request request
    ) {
    }

    @Override
    public void logIncomingRequestAtNode(
        final QueryContext context, final FullQuery.Request request
    ) {
    }

    @Override
    public void logOutgoingResponseAtNode(final QueryContext context, final FullQuery response) {
    }

    @Override
    public void logIncomingResponseFromShard(
        final QueryContext context, final FullQuery response
    ) {
    }

    @Override
    public void logFinalResponse(
        final QueryContext context, final QueryMetricsResponse queryMetricsResponse
    ) {
    }
}
