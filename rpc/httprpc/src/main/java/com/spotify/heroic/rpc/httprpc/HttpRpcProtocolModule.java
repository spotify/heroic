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

package com.spotify.heroic.rpc.httprpc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.spotify.heroic.HeroicOptions;
import com.spotify.heroic.cluster.RpcProtocol;
import com.spotify.heroic.cluster.RpcProtocolModule;

import lombok.Data;

@Data
public class HttpRpcProtocolModule implements RpcProtocolModule {
    @JsonCreator
    public static HttpRpcProtocolModule create() {
        return new HttpRpcProtocolModule();
    }

    public static HttpRpcProtocolModule createDefault() {
        return create();
    }

    @Override
    public Module module(final Key<RpcProtocol> key, final HeroicOptions options) {
        return new PrivateModule() {
            @Override
            protected void configure() {
                bind(key).to(HttpRpcProtocol.class).in(Scopes.SINGLETON);
                expose(key);
            }
        };
    }

    @Override
    public String scheme() {
        return "http";
    }
}