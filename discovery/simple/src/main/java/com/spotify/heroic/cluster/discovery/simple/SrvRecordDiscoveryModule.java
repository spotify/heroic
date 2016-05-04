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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.cluster.ClusterDiscoveryComponent;
import com.spotify.heroic.cluster.ClusterDiscoveryModule;
import com.spotify.heroic.dagger.PrimaryComponent;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import lombok.Data;

import javax.inject.Named;
import java.util.List;
import java.util.Optional;

@Data
public class SrvRecordDiscoveryModule implements ClusterDiscoveryModule {
    private final List<String> records;
    private final Optional<String> protocol;
    private final Optional<Integer> port;

    @JsonCreator
    public SrvRecordDiscoveryModule(
        @JsonProperty("records") List<String> records, @JsonProperty("protocol") String protocol,
        @JsonProperty("port") Integer port
    ) {
        this.records = Optional.ofNullable(records).orElseGet(ImmutableList::of);
        this.protocol = Optional.ofNullable(protocol);
        this.port = Optional.ofNullable(port);
    }

    @Override
    public ClusterDiscoveryComponent module(PrimaryComponent primary) {
        return DaggerSrvRecordDiscoveryModule_C
            .builder()
            .primaryComponent(primary)
            .m(new M())
            .build();
    }

    @DiscoveryScope
    @Component(modules = M.class, dependencies = PrimaryComponent.class)
    interface C extends ClusterDiscoveryComponent {
        @Override
        SrvRecordDiscovery clusterDiscovery();
    }

    @Module
    class M {
        @Provides
        @Named("records")
        @DiscoveryScope
        public List<String> records() {
            return records;
        }

        @Provides
        @Named("protocol")
        @DiscoveryScope
        public Optional<String> protocol() {
            return protocol;
        }

        @Provides
        @Named("port")
        @DiscoveryScope
        public Optional<Integer> port() {
            return port;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private ImmutableList.Builder<String> records = ImmutableList.builder();
        private String protocol;
        private Integer port;

        public Builder records(final List<String> records) {
            this.records = ImmutableList.<String>builder().addAll(records);
            return this;
        }

        public Builder protocol(final String protocol) {
            this.protocol = protocol;
            return this;
        }

        public Builder port(final int port) {
            this.port = port;
            return this;
        }

        public SrvRecordDiscoveryModule build() {
            return new SrvRecordDiscoveryModule(records.build(), protocol, port);
        }
    }

    public static ClusterDiscoveryModule createDefault() {
        return new SrvRecordDiscoveryModule(null, null, null);
    }
}
