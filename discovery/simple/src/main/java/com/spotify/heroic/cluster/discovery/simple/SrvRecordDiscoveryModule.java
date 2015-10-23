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

import java.util.List;
import java.util.Optional;

import javax.inject.Named;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.spotify.heroic.cluster.ClusterDiscovery;
import com.spotify.heroic.cluster.ClusterDiscoveryModule;

import lombok.Data;

@Data
public class SrvRecordDiscoveryModule implements ClusterDiscoveryModule {
    private final List<String> records;
    private final Optional<String> protocol;
    private final Optional<Integer> port;

    @JsonCreator
    public SrvRecordDiscoveryModule(@JsonProperty("records") List<String> records, @JsonProperty("protocol") String protocol, @JsonProperty("port") Integer port) {
        this.records = Optional.ofNullable(records).orElseGet(ImmutableList::of);
        this.protocol = Optional.ofNullable(protocol);
        this.port = Optional.ofNullable(port);
    }

    @Override
    public Module module(final Key<ClusterDiscovery> key) {
        return new PrivateModule() {
            @Provides
            @Named("records")
            public List<String> records() {
                return records;
            }

            @Provides
            @Named("protocol")
            public Optional<String> protocol() {
                return protocol;
            }

            @Provides
            @Named("port")
            public Optional<Integer> port() {
                return port;
            }

            @Override
            protected void configure() {
                bind(key).to(SrvRecordDiscovery.class);
                expose(key);
            }
        };
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