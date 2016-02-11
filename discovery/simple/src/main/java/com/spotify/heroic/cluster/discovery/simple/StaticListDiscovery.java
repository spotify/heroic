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

import com.spotify.heroic.cluster.ClusterDiscovery;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.ToString;

import javax.inject.Inject;
import javax.inject.Named;
import java.net.URI;
import java.util.List;

@ToString
public class StaticListDiscovery implements ClusterDiscovery {
    private final AsyncFramework async;
    private final List<URI> nodes;

    @Inject
    public StaticListDiscovery(AsyncFramework async, @Named("nodes") List<URI> nodes) {
        this.async = async;
        this.nodes = nodes;
    }

    @Override
    public AsyncFuture<List<URI>> find() {
        return async.resolved(nodes);
    }
}
