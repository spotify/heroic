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

package com.spotify.heroic.metric.datastax;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.inject.Named;
import javax.inject.Singleton;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.concurrrency.ReadWriteThreadPools;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.statistics.LocalMetricManagerReporter;
import com.spotify.heroic.statistics.MetricBackendReporter;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.Managed;
import lombok.Data;

@Data
public final class DatastaxMetricModule implements MetricModule {
    public static final Set<String> DEFAULT_SEEDS = ImmutableSet.of("localhost");
    public static final String DEFAULT_KEYSPACE = "heroic";
    public static final String DEFAULT_GROUP = "heroic";
    public static final int DEFAULT_PORT = 9042;

    private final String id;
    private final Groups groups;
    private final String keyspace;
    private final List<InetSocketAddress> seeds;
    private final ReadWriteThreadPools.Config pools;

    @JsonCreator
    public DatastaxMetricModule(@JsonProperty("id") String id, @JsonProperty("seeds") Set<String> seeds,
            @JsonProperty("keyspace") String keyspace, @JsonProperty("group") String group,
            @JsonProperty("groups") Set<String> groups, @JsonProperty("pools") ReadWriteThreadPools.Config pools) {
        this.id = id;
        this.groups = Groups.groups(group, groups, DEFAULT_GROUP);
        this.keyspace = Optional.fromNullable(keyspace).or(DEFAULT_KEYSPACE);
        this.seeds = convert(Optional.fromNullable(seeds).or(DEFAULT_SEEDS));
        this.pools = Optional.fromNullable(pools).or(ReadWriteThreadPools.Config.provideDefault());
    }

    private static List<InetSocketAddress> convert(Set<String> source) {
        final List<InetSocketAddress> seeds = new ArrayList<>();

        for (final String s : source) {
            seeds.add(convert(s));
        }

        if (seeds.isEmpty())
            throw new IllegalArgumentException("No seeds specified");

        return seeds;
    }

    private static InetSocketAddress convert(String s) {
        final URI u;

        try {
            u = new URI("custom://" + s);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("invalid seed address '" + s + "'", e);
        }

        final String host = u.getHost();
        final int port = u.getPort() != -1 ? u.getPort() : DEFAULT_PORT;

        if (host == null)
            throw new IllegalArgumentException("invalid seed address '" + s + "', no host specified");

        return new InetSocketAddress(host, port);
    }

    @Override
    public PrivateModule module(final Key<MetricBackend> key, final String id) {
        return new PrivateModule() {
            @Provides
            @Singleton
            public MetricBackendReporter reporter(LocalMetricManagerReporter reporter) {
                return reporter.newBackend(id);
            }

            @Provides
            @Singleton
            public ReadWriteThreadPools pools(AsyncFramework async, MetricBackendReporter reporter) {
                return pools.construct(async, reporter.newThreadPool());
            }

            @Provides
            @Singleton
            public Groups groups() {
                return groups;
            }

            @Provides
            @Singleton
            public Managed<Connection> connection(final AsyncFramework async) {
                return async.managed(new ManagedSetupConnection(async, seeds, keyspace));
            }

            @Override
            protected void configure() {
                bind(key).to(DatastaxBackend.class).in(Scopes.SINGLETON);
                expose(key);
            }
        };
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public String buildId(int i) {
        return String.format("heroic#%d", i);
    }
}
