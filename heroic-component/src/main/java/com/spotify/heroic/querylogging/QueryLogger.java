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

import com.spotify.heroic.Query;
import com.spotify.heroic.metric.FullQuery;
import com.spotify.heroic.metric.QueryMetrics;
import com.spotify.heroic.metric.QueryMetricsResponse;

public interface QueryLogger {

    void logHttpQueryText(QueryContext context, String queryString);

    void logHttpQueryJson(QueryContext context, QueryMetrics query);

    void logQuery(QueryContext context, Query query);

    void logOutgoingRequestToShards(QueryContext context, FullQuery.Request request);

    void logIncomingRequestAtNode(QueryContext context, FullQuery.Request request);

    void logOutgoingResponseAtNode(QueryContext context, FullQuery response);

    void logIncomingResponseFromShard(QueryContext context, FullQuery response);

    void logFinalResponse(QueryContext context, QueryMetricsResponse queryMetricsResponse);
}
