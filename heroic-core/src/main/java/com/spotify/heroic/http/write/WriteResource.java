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

package com.spotify.heroic.http.write;

import com.spotify.heroic.common.JavaxRestFramework;
import com.spotify.heroic.ingestion.IngestionManager;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

@Path("write")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class WriteResource {
    private final IngestionManager ingestion;
    private final JavaxRestFramework httpAsync;

    @Inject
    public WriteResource(final IngestionManager ingestion, final JavaxRestFramework httpAsync) {
        this.ingestion = ingestion;
        this.httpAsync = httpAsync;
    }

    @POST
    public void metrics(
        @Suspended final AsyncResponse response, @QueryParam("group") String group,
        WriteMetricRequest write
    ) throws Exception {
        httpAsync.bind(response, ingestion.useGroup(group).write(write.toWriteMetric()), r -> r);
    }
}
