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

package com.spotify.heroic.metric.bigtable;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;
import java.util.concurrent.Callable;

import javax.inject.Singleton;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.name.Named;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.metric.bigtable.credentials.ComputeEngineCredentialsBuilder;
import com.spotify.heroic.statistics.LocalMetricManagerReporter;
import com.spotify.heroic.statistics.MetricBackendReporter;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;
import lombok.Data;

@Data
public final class BigtableMetricModule implements MetricModule {
    public static final String DEFAULT_GROUP = "bigtable";
    public static final String DEFAULT_CLUSTER = "heroic";
    public static final CredentialsBuilder DEFAULT_CREDENTIALS = new ComputeEngineCredentialsBuilder();

    private final String id;
    private final Groups groups;
    private final String project;
    private final String zone;
    private final String cluster;
    private final CredentialsBuilder credentials;

    @JsonCreator
    public BigtableMetricModule(@JsonProperty("id") String id, @JsonProperty("group") String group,
            @JsonProperty("groups") Set<String> groups, @JsonProperty("project") String project,
            @JsonProperty("zone") String zone, @JsonProperty("cluster") String cluster,
            @JsonProperty("credentials") CredentialsBuilder credentials) {
        this.id = id;
        this.groups = Groups.groups(group, groups, DEFAULT_GROUP);
        this.project = checkNotNull(project, "project");
        this.zone = checkNotNull(zone, "zone");
        this.cluster = Optional.fromNullable(cluster).or(DEFAULT_CLUSTER);
        this.credentials = Optional.fromNullable(credentials).or(DEFAULT_CREDENTIALS);
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
            public Managed<BigtableConnection> connection(final AsyncFramework async) {
                return async.managed(new ManagedSetup<BigtableConnection>() {
                    @Override
                    public AsyncFuture<BigtableConnection> construct() throws Exception {
                        return async.call(new BigtableConnectionBuilder(project, zone, cluster, credentials, async));
                    }

                    @Override
                    public AsyncFuture<Void> destruct(final BigtableConnection value) throws Exception {
                        return async.call(new Callable<Void>() {
                            @Override
                            public Void call() throws Exception {
                                value.close();
                                return null;
                            }
                        });
                    }
                });
            }

            @Provides
            @Singleton
            public Serializer<RowKey> rowKeySerializer(Serializer<Series> series,
                    @Named("common") SerializerFramework s) {
                return new RowKey_Serializer(s);
            }

            @Provides
            @Singleton
            public Groups groups() {
                return groups;
            }

            @Override
            protected void configure() {
                bind(key).to(BigtableBackend.class).in(Scopes.SINGLETON);
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
        return String.format("bigtable#%d", i);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String id;
        private String group;
        private Set<String> groups;
        private String project;
        private String zone;
        private String cluster;
        private CredentialsBuilder credentials;

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder group(String group) {
            this.group = group;
            return this;
        }

        public Builder groups(Set<String> groups) {
            this.groups = groups;
            return this;
        }

        public Builder project(String project) {
            this.project = checkNotNull(project, "project");
            return this;
        }

        public Builder zone(String zone) {
            this.zone = checkNotNull(zone, "zone");
            return this;
        }

        public Builder cluster(String cluster) {
            this.cluster = checkNotNull(cluster, "cluster");
            return this;
        }

        public Builder credentials(CredentialsBuilder credentials) {
            this.credentials = checkNotNull(credentials, "credentials");
            return this;
        }

        public BigtableMetricModule build() {
            return new BigtableMetricModule(id, group, groups, project, zone, cluster, credentials);
        }
    }
}
