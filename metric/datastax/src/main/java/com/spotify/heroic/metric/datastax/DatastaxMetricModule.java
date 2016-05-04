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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.ExtraParameters;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.lifecycle.LifeCycleManager;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.metric.datastax.schema.Schema;
import com.spotify.heroic.metric.datastax.schema.SchemaComponent;
import com.spotify.heroic.metric.datastax.schema.SchemaModule;
import com.spotify.heroic.metric.datastax.schema.ng.NextGenSchemaModule;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.Managed;
import lombok.Data;

import javax.inject.Named;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Optional.empty;
import static java.util.Optional.of;

@Data
public final class DatastaxMetricModule implements MetricModule {
    public static final String DATASTAX_CONFIGURE = "datastax.configure";

    public static final Set<String> DEFAULT_SEEDS = ImmutableSet.of("localhost");
    public static final String DEFAULT_GROUP = "heroic";
    public static final int DEFAULT_PORT = 9042;
    public static final boolean DEFAULT_CONFIGURE = false;
    public static final int DEFAULT_FETCH_SIZE = 5000;
    public static final Duration DEFAULT_READ_TIMEOUT = new Duration(30, TimeUnit.SECONDS);

    /* id of backend (defualt will be generated) */
    private final Optional<String> id;
    /* groups for this backend */
    private final Groups groups;
    /* database seeds */
    private final List<InetSocketAddress> seeds;
    /* row key serialization method to use */
    private final SchemaModule schema;
    /* automatically configure database */
    private final boolean configure;
    /* the default number of rows to fetch in a single batch */
    private final int fetchSize;
    /* the default read timeout for queries */
    private final Duration readTimeout;
    /* the default consistency level to use */
    private final ConsistencyLevel consistencyLevel;
    /* the retry policy to use */
    private final RetryPolicy retryPolicy;
    /* authentication to apply to builder */
    private final DatastaxAuthentication authentication;

    @JsonCreator
    public DatastaxMetricModule(
        @JsonProperty("id") Optional<String> id, @JsonProperty("groups") Optional<Groups> groups,
        @JsonProperty("seeds") Optional<Set<String>> seeds,
        @JsonProperty("schema") Optional<SchemaModule> schema,
        @JsonProperty("configure") Optional<Boolean> configure,
        @JsonProperty("fetchSize") Optional<Integer> fetchSize,
        @JsonProperty("readTimeout") Optional<Duration> readTimeout,
        @JsonProperty("consistencyLevel") Optional<ConsistencyLevel> consistencyLevel,
        @JsonProperty("retryPolicy") Optional<RetryPolicy> retryPolicy,
        @JsonProperty("authentication") Optional<DatastaxAuthentication> authentication
    ) {
        this.id = id;
        this.groups = groups.orElseGet(Groups::empty).or("heroic");
        this.seeds = convert(seeds.orElse(DEFAULT_SEEDS));
        this.schema = schema.orElseGet(NextGenSchemaModule.builder()::build);
        this.configure = configure.orElse(DEFAULT_CONFIGURE);
        this.fetchSize = fetchSize.orElse(DEFAULT_FETCH_SIZE);
        this.readTimeout = readTimeout.orElse(DEFAULT_READ_TIMEOUT);
        this.consistencyLevel = consistencyLevel.orElse(ConsistencyLevel.ONE);
        this.retryPolicy = retryPolicy.orElse(DefaultRetryPolicy.INSTANCE);
        this.authentication = authentication.orElseGet(DatastaxAuthentication.None::new);
    }

    private static List<InetSocketAddress> convert(Set<String> source) {
        final List<InetSocketAddress> seeds = new ArrayList<>();

        for (final String s : source) {
            seeds.add(convert(s));
        }

        if (seeds.isEmpty()) {
            throw new IllegalArgumentException("No seeds specified");
        }

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

        if (host == null) {
            throw new IllegalArgumentException(
                "invalid seed address '" + s + "', no host specified");
        }

        return new InetSocketAddress(host, port);
    }

    @Override
    public Exposed module(PrimaryComponent primary, Depends depends, String id) {
        final SchemaComponent schema = this.schema.module(primary);

        return DaggerDatastaxMetricModule_C
            .builder()
            .primaryComponent(primary)
            .schemaComponent(schema)
            .depends(depends)
            .m(new M())
            .build();
    }

    @DatastaxScope
    @Component(modules = M.class,
        dependencies = {PrimaryComponent.class, Depends.class, SchemaComponent.class})
    interface C extends Exposed {
        @Override
        DatastaxBackend backend();

        @Override
        LifeCycle life();
    }

    @Module
    class M {
        @Provides
        @DatastaxScope
        @Named("configure")
        public boolean configure(final ExtraParameters params) {
            return params.containsAny(ExtraParameters.CONFIGURE.getName(), DATASTAX_CONFIGURE) ||
                configure;
        }

        @Provides
        @DatastaxScope
        public Groups groups() {
            return groups;
        }

        @Provides
        @DatastaxScope
        public Managed<Connection> connection(
            final AsyncFramework async, @Named("configure") final boolean configure,
            final Schema schema
        ) {
            return async.managed(
                new ManagedSetupConnection(async, seeds, schema, configure, fetchSize, readTimeout,
                    consistencyLevel, retryPolicy, authentication));
        }

        @Provides
        @DatastaxScope
        LifeCycle life(LifeCycleManager manager, DatastaxBackend backend) {
            return manager.build(backend);
        }
    }

    @Override
    public Optional<String> id() {
        return id;
    }

    @Override
    public String buildId(int i) {
        return String.format("heroic#%d", i);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Optional<String> id = empty();
        private Optional<Groups> groups = empty();
        private Optional<Set<String>> seeds = empty();
        private Optional<SchemaModule> schema = empty();
        private Optional<Boolean> configure = empty();
        private Optional<Integer> fetchSize = empty();
        private Optional<Duration> readTimeout = empty();
        private Optional<ConsistencyLevel> consistencyLevel = empty();
        private Optional<RetryPolicy> retryPolicy = empty();
        private Optional<DatastaxAuthentication> authentication = empty();

        public Builder id(String id) {
            this.id = of(id);
            return this;
        }

        public Builder groups(Groups groups) {
            this.groups = of(groups);
            return this;
        }

        public Builder seeds(Set<String> seeds) {
            this.seeds = of(seeds);
            return this;
        }

        public Builder schema(SchemaModule schema) {
            this.schema = of(schema);
            return this;
        }

        public Builder configure(boolean configure) {
            this.configure = of(configure);
            return this;
        }

        public Builder fetchSize(int fetchSize) {
            this.fetchSize = of(fetchSize);
            return this;
        }

        public Builder readTimeout(Duration readTimeout) {
            this.readTimeout = of(readTimeout);
            return this;
        }

        public Builder consistencyLevel(ConsistencyLevel consistencyLevel) {
            this.consistencyLevel = of(consistencyLevel);
            return this;
        }

        public Builder retryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = of(retryPolicy);
            return this;
        }

        public Builder authentication(DatastaxAuthentication authentication) {
            this.authentication = of(authentication);
            return this;
        }

        public DatastaxMetricModule build() {
            return new DatastaxMetricModule(id, groups, seeds, schema, configure, fetchSize,
                readTimeout, consistencyLevel, retryPolicy, authentication);
        }
    }
}
