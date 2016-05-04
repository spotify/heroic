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

import com.spotify.heroic.dagger.PrimaryComponent;
import dagger.Component;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.Data;

import javax.inject.Inject;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public interface ClusterDiscoveryModule {
    ClusterDiscoveryComponent module(PrimaryComponent primary);

    static ClusterDiscoveryModule nullModule() {
        return new ClusterDiscoveryModule() {
            @Override
            public ClusterDiscoveryComponent module(PrimaryComponent primary) {
                return DaggerClusterDiscoveryModule_NullComponent
                    .builder()
                    .primaryComponent(primary)
                    .build();
            }
        };
    }

    @ClusterScope
    @Component(dependencies = PrimaryComponent.class)
    interface NullComponent extends ClusterDiscoveryComponent {
        @Override
        Null clusterDiscovery();
    }

    @Data
    @ClusterScope
    class Null implements ClusterDiscovery {
        private final AsyncFramework async;

        @Inject
        public Null(AsyncFramework async) {
            this.async = async;
        }

        @Override
        public AsyncFuture<List<URI>> find() {
            return async.<List<URI>>resolved(new ArrayList<URI>());
        }
    }
}
