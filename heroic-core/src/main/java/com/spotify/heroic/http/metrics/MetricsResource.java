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

package com.spotify.heroic.http.metrics;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

import com.google.inject.Inject;
import com.spotify.heroic.common.JavaxRestFramework;
import com.spotify.heroic.metric.BackendKeySet;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.QueryOptions;

import eu.toolchain.async.AsyncFuture;

@Path("metrics")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MetricsResource {
    private final JavaxRestFramework.Resume<BackendKeySet, BackendKeySet> KEYS = new JavaxRestFramework.Resume<BackendKeySet, BackendKeySet>() {
        @Override
        public BackendKeySet resume(BackendKeySet result) throws Exception {
            return result;
        }
    };

    @Inject
    private JavaxRestFramework httpAsync;

    @Inject
    private MetricManager metrics;

    @GET
    @Path("keys")
    public void metrics(@Suspended final AsyncResponse response, @QueryParam("group") String group,
            @QueryParam("limit") Integer limit) throws Exception {
        final AsyncFuture<BackendKeySet> keys = metrics.useGroup(group).keys(null,
                limit == null ? 1000 : limit, QueryOptions.defaults());
        httpAsync.bind(response, keys, KEYS);
    }
}