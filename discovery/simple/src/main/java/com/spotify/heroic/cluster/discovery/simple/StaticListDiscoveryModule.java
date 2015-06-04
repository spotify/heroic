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

package com.spotify.heroic.cluster.discovery.simple;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Named;

import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.spotify.heroic.cluster.ClusterDiscovery;
import com.spotify.heroic.cluster.ClusterDiscoveryModule;

@RequiredArgsConstructor
public class StaticListDiscoveryModule implements ClusterDiscoveryModule {
    private static final int DEFAULT_THREAD_POOL_SIZE = 100;

    private final List<URI> nodes;

    @JsonCreator
    public static StaticListDiscoveryModule create(@JsonProperty("threadPoolSize") Integer threadPoolSize,
            @JsonProperty("nodes") List<URI> nodes) {
        if (threadPoolSize == null)
            threadPoolSize = DEFAULT_THREAD_POOL_SIZE;

        if (nodes == null)
            nodes = new ArrayList<>();

        return new StaticListDiscoveryModule(nodes);
    }

    @Override
    public Module module(final Key<ClusterDiscovery> key) {
        return new PrivateModule() {
            @Provides
            @Named("nodes")
            public List<URI> nodes() {
                return nodes;
            }

            @Override
            protected void configure() {
                bind(key).to(StaticListDiscovery.class);
                expose(key);
            }
        };
    }

    public static ClusterDiscoveryModule createDefault() {
        return create(null, null);
    }
}
