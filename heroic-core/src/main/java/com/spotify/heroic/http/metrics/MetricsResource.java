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

import java.util.List;

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
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.MetricManager;

import eu.toolchain.async.AsyncFuture;

@Path("metrics")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MetricsResource {
    private final JavaxRestFramework.Resume<List<BackendKey>, List<BackendKey>> KEYS = new JavaxRestFramework.Resume<List<BackendKey>, List<BackendKey>>() {
        @Override
        public List<BackendKey> resume(List<BackendKey> result) throws Exception {
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
        final AsyncFuture<List<BackendKey>> keys = metrics.useGroup(group).keys(null, null,
                limit == null ? 1000 : limit);
        httpAsync.bind(response, keys, KEYS);
    }
}