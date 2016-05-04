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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.ExtraParameters;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.lifecycle.LifeCycleManager;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.metric.bigtable.credentials.ComputeEngineCredentialsBuilder;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;
import lombok.Data;

import javax.inject.Named;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static java.util.Optional.empty;
import static java.util.Optional.of;

@Data
public final class BigtableMetricModule implements MetricModule {
    public static final String BIGTABLE_CONFIGURE_PARAM = "bigtable.configure";

    public static final String DEFAULT_GROUP = "bigtable";
    public static final String DEFAULT_CLUSTER = "heroic";
    public static final CredentialsBuilder DEFAULT_CREDENTIALS =
        new ComputeEngineCredentialsBuilder();

    private final Optional<String> id;
    private final Groups groups;
    private final String project;
    private final String zone;
    private final String cluster;
    private final CredentialsBuilder credentials;

    @JsonCreator
    public BigtableMetricModule(
        @JsonProperty("id") Optional<String> id, @JsonProperty("groups") Optional<Groups> groups,
        @JsonProperty("project") Optional<String> project,
        @JsonProperty("zone") Optional<String> zone,
        @JsonProperty("cluster") Optional<String> cluster,
        @JsonProperty("credentials") Optional<CredentialsBuilder> credentials
    ) {
        this.id = id;
        this.groups = groups.orElseGet(Groups::empty).or(DEFAULT_GROUP);
        this.project = project.orElseThrow(() -> new NullPointerException("project"));
        this.zone = zone.orElseThrow(() -> new NullPointerException("zone"));
        this.cluster = cluster.orElse(DEFAULT_CLUSTER);
        this.credentials = credentials.orElse(DEFAULT_CREDENTIALS);
    }

    @Override
    public Exposed module(PrimaryComponent primary, Depends depends, String id) {
        return DaggerBigtableMetricModule_C
            .builder()
            .primaryComponent(primary)
            .depends(depends)
            .m(new M())
            .build();
    }

    @BigtableScope
    @Component(modules = M.class, dependencies = {PrimaryComponent.class, Depends.class})
    interface C extends Exposed {
        @Override
        BigtableBackend backend();

        @Override
        LifeCycle life();
    }

    @Module
    class M {
        @Provides
        @BigtableScope
        public Managed<BigtableConnection> connection(
            final AsyncFramework async, final ExecutorService executorService
        ) {
            return async.managed(new ManagedSetup<BigtableConnection>() {
                @Override
                public AsyncFuture<BigtableConnection> construct() throws Exception {
                    return async.call(
                        new BigtableConnectionBuilder(project, zone, cluster, credentials, async,
                            executorService));
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
        @BigtableScope
        @Named("configure")
        public boolean configure(final ExtraParameters params) {
            return params.contains(ExtraParameters.CONFIGURE) ||
                params.contains(BIGTABLE_CONFIGURE_PARAM);
        }

        @Provides
        @BigtableScope
        public Serializer<RowKey> rowKeySerializer(
            Serializer<Series> series, @Named("common") SerializerFramework s
        ) {
            return new RowKey_Serializer(s);
        }

        @Provides
        @BigtableScope
        public Groups groups() {
            return groups;
        }

        @Provides
        @BigtableScope
        public LifeCycle life(LifeCycleManager manager, BigtableBackend backend) {
            return manager.build(backend);
        }
    }

    @Override
    public Optional<String> id() {
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
        private Optional<String> id = empty();
        private Optional<Groups> groups = empty();
        private Optional<String> project = empty();
        private Optional<String> zone = empty();
        private Optional<String> cluster = empty();
        private Optional<CredentialsBuilder> credentials = empty();

        public Builder id(String id) {
            this.id = of(id);
            return this;
        }

        public Builder groups(Groups groups) {
            this.groups = of(groups);
            return this;
        }

        public Builder project(String project) {
            this.project = of(project);
            return this;
        }

        public Builder zone(String zone) {
            this.zone = of(zone);
            return this;
        }

        public Builder cluster(String cluster) {
            this.cluster = of(cluster);
            return this;
        }

        public Builder credentials(CredentialsBuilder credentials) {
            this.credentials = of(credentials);
            return this;
        }

        public BigtableMetricModule build() {
            return new BigtableMetricModule(id, groups, project, zone, cluster, credentials);
        }
    }
}
