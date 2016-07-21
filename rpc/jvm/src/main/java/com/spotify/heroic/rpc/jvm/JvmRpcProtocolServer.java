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

package com.spotify.heroic.rpc.jvm;

import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.LocalClusterNode;
import com.spotify.heroic.lifecycle.LifeCycleRegistry;
import com.spotify.heroic.lifecycle.LifeCycles;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Named;

@Slf4j
public class JvmRpcProtocolServer implements LifeCycles {
    private final AsyncFramework async;
    private final ClusterNode localNode;
    private final String bindName;
    private final JvmRpcContext context;

    @Inject
    public JvmRpcProtocolServer(
        AsyncFramework async, LocalClusterNode localNode, @Named("bindName") String bindName,
        JvmRpcContext context
    ) {
        this.async = async;
        this.localNode = localNode;
        this.bindName = bindName;
        this.context = context;
    }

    @Override
    public void register(LifeCycleRegistry registry) {
        registry.start(this::start);
        registry.stop(this::stop);
    }

    private AsyncFuture<Void> start() {
        if (!context.bind(bindName, localNode)) {
            return async.failed(
                new RuntimeException("Could not bind (" + bindName + "), name already used"));
        }

        return async.resolved();
    }

    private AsyncFuture<Void> stop() {
        if (!context.unbind(bindName)) {
            return async.failed(
                new RuntimeException("Could not unbind (" + bindName + "), name not registered"));
        }

        return async.resolved();
    }
}
