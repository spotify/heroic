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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.cluster.NodeMetadata;
import com.spotify.heroic.cluster.RpcProtocolComponent;
import com.spotify.heroic.cluster.RpcProtocolModule;
import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.lifecycle.LifeCycleManager;
import com.spotify.heroic.metadata.MetadataComponent;
import com.spotify.heroic.metric.MetricComponent;
import com.spotify.heroic.suggest.SuggestComponent;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Data
public class JvmRpcProtocolModule implements RpcProtocolModule {
    private static final String DEFAULT_BIND_NAME = "heroic-jvm";

    private final String bindName;
    private final JvmRpcContext context;

    @JsonCreator
    public JvmRpcProtocolModule(
        @JsonProperty("bindName") Optional<String> bindName, Optional<JvmRpcContext> context
    ) {
        this.bindName = bindName.orElse(DEFAULT_BIND_NAME);
        this.context = context.orElseGet(JvmRpcContext::globalContext);
    }

    @Override
    public RpcProtocolComponent module(
        PrimaryComponent primary, MetricComponent metric, MetadataComponent metadata,
        SuggestComponent suggest, NodeMetadata nodeMetadata
    ) {
        return DaggerJvmRpcProtocolModule_C
            .builder()
            .primaryComponent(primary)
            .metricComponent(metric)
            .metadataComponent(metadata)
            .suggestComponent(suggest)
            .m(new M(nodeMetadata))
            .build();
    }

    @JvmRpcScope
    @Component(modules = M.class, dependencies = {
        PrimaryComponent.class, SuggestComponent.class, MetricComponent.class,
        MetadataComponent.class
    })
    interface C extends RpcProtocolComponent {
        @Override
        JvmRpcProtocol rpcProtocol();

        @Override
        LifeCycle life();
    }

    @RequiredArgsConstructor
    @Module
    class M {
        private final NodeMetadata nodeMetadata;

        @Provides
        @JvmRpcScope
        public NodeMetadata nodeMetadata() {
            return nodeMetadata;
        }

        @Provides
        @JvmRpcScope
        public JvmRpcContext context() {
            return context;
        }

        @Provides
        @JvmRpcScope
        @Named("bindName")
        String bindName() {
            return bindName;
        }

        @Provides
        @JvmRpcScope
        LifeCycle server(
            LifeCycleManager manager, JvmRpcProtocolServer server
        ) {
            final List<LifeCycle> life = new ArrayList<>();
            life.add(manager.build(server));
            return LifeCycle.combined(life);
        }
    }

    @Override
    public String scheme() {
        return "jvm";
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Optional<String> bindName = Optional.empty();
        private Optional<JvmRpcContext> context = Optional.empty();

        public Builder bindName(final String bindName) {
            this.bindName = Optional.of(bindName);
            return this;
        }

        public Builder context(final JvmRpcContext context) {
            this.context = Optional.of(context);
            return this;
        }

        public JvmRpcProtocolModule build() {
            return new JvmRpcProtocolModule(bindName, context);
        }
    }
}
