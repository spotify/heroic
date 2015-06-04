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

package com.spotify.heroic.http;

import com.google.inject.Inject;
import com.spotify.heroic.HeroicConfigurationContext;
import com.spotify.heroic.HeroicModule;
import com.spotify.heroic.http.cluster.ClusterResource;
import com.spotify.heroic.http.metadata.MetadataResource;
import com.spotify.heroic.http.metadata.MetadataResourceModule;
import com.spotify.heroic.http.metrics.MetricsResource;
import com.spotify.heroic.http.parser.ParserResource;
import com.spotify.heroic.http.query.QueryResource;
import com.spotify.heroic.http.render.RenderResource;
import com.spotify.heroic.http.status.StatusResource;
import com.spotify.heroic.http.utils.UtilsResource;
import com.spotify.heroic.http.write.WriteResource;

public class Entry implements HeroicModule {
    @Inject
    private HeroicConfigurationContext config;

    @Override
    public void setup() {
        config.resource(HeroicResource.class);
        config.resource(WriteResource.class);
        config.resource(UtilsResource.class);
        config.resource(StatusResource.class);
        config.resource(RenderResource.class);
        config.resource(QueryResource.class);
        config.resource(MetadataResource.class);
        config.module(new MetadataResourceModule());
        config.resource(ClusterResource.class);
        config.resource(MetricsResource.class);
        config.resource(ParserResource.class);

        config.resource(ErrorMapper.class);
        config.resource(ParseExceptionMapper.class);
        config.resource(CustomExceptionMapper.class);
        config.resource(UnrecognizedPropertyExceptionMapper.class);
    }
}
