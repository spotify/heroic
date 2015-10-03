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

package com.spotify.heroic.cluster;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Singleton;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.Data;

public interface ClusterDiscoveryModule {
    public Module module(final Key<ClusterDiscovery> key);

    public static ClusterDiscoveryModule nullModule() {
        return new ClusterDiscoveryModule() {
            @Override
            public Module module(Key<ClusterDiscovery> key) {
                return new PrivateModule() {
                    @Override
                    protected void configure() {
                        bind(key).to(Null.class);
                        expose(key);
                    }
                };
            }
        };
    }

    @Data
    @Singleton
    public static class Null implements ClusterDiscovery {
        @Inject
        private AsyncFramework async;

        @Override
        public AsyncFuture<List<URI>> find() {
            return async.<List<URI>> resolved(new ArrayList<URI>());
        }
    }
}