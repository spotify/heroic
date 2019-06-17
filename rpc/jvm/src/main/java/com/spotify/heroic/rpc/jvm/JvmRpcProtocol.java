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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.RpcProtocol;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import java.net.URI;
import javax.inject.Inject;
import javax.inject.Named;

@JvmRpcScope
public class JvmRpcProtocol implements RpcProtocol {
    private final AsyncFramework async;
    private final ObjectMapper mapper;
    private final String bindName;
    private final JvmRpcContext context;

    @Inject
    public JvmRpcProtocol(
        AsyncFramework async, @Named("application/json+internal") ObjectMapper mapper,
        @Named("bindName") String bindName, JvmRpcContext context
    ) {
        this.async = async;
        this.mapper = mapper;
        this.bindName = bindName;
        this.context = context;
    }

    @Override
    public AsyncFuture<ClusterNode> connect(final URI uri) {
        return context
            .resolve(uri.getHost())
            .map(async::resolved)
            .orElseGet(() -> async.failed(new RuntimeException("Connection refused to: " + uri)));
    }

    @Override
    public AsyncFuture<String> getListenURI() {
        return async.resolved(bindName);
    }

    public String toString() {
        return "JvmRpcProtocol()";
    }
}
