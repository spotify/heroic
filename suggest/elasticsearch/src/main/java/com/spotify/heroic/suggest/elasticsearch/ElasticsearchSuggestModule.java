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

package com.spotify.heroic.suggest.elasticsearch;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.inject.Singleton;

import lombok.ToString;

import org.elasticsearch.common.xcontent.json.JsonXContent;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.spotify.heroic.concurrrency.ReadWriteThreadPools;
import com.spotify.heroic.elasticsearch.Connection;
import com.spotify.heroic.elasticsearch.ManagedConnectionFactory;
import com.spotify.heroic.statistics.LocalMetadataBackendReporter;
import com.spotify.heroic.statistics.LocalMetadataManagerReporter;
import com.spotify.heroic.suggest.SuggestBackend;
import com.spotify.heroic.suggest.SuggestModule;
import com.spotify.heroic.utils.GroupedUtils;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.Managed;

@ToString
public final class ElasticsearchSuggestModule implements SuggestModule {
    public static final String DEFAULT_GROUP = "elasticsearch";
    public static final String TEMPLATE_NAME = "heroic-suggest";

    private final String id;
    private final Set<String> groups;
    private final ReadWriteThreadPools.Config pools;
    private final ManagedConnectionFactory connection;

    @JsonCreator
    public ElasticsearchSuggestModule(@JsonProperty("id") String id, @JsonProperty("group") String group,
            @JsonProperty("groups") Set<String> groups,
            @JsonProperty("connection") ManagedConnectionFactory connection,
            @JsonProperty("pools") ReadWriteThreadPools.Config pools) {
        this.id = id;
        this.groups = GroupedUtils.groups(group, groups, DEFAULT_GROUP);
        this.connection = Optional.fromNullable(connection).or(ManagedConnectionFactory.provideDefault());
        this.pools = Optional.fromNullable(pools).or(ReadWriteThreadPools.Config.provideDefault());
    }

    private Map<String, Map<String, Object>> mappings() throws IOException {
        final Map<String, Map<String, Object>> mappings = new HashMap<>();
        mappings.put("tag", JsonXContent.jsonXContent.createParser(inputStreamFromResource("tag.v1.json")).map());
        mappings.put("series", JsonXContent.jsonXContent.createParser(inputStreamFromResource("series.v1.json")).map());
        return mappings;
    }

    private InputStream inputStreamFromResource(String string) {
        return getClass().getClassLoader().getResourceAsStream(string);
    }

    @Override
    public Module module(final Key<SuggestBackend> key, final String id) {
        return new PrivateModule() {
            @Provides
            @Singleton
            public LocalMetadataBackendReporter reporter(LocalMetadataManagerReporter reporter) {
                return reporter.newMetadataBackend(id);
            }

            @Provides
            @Singleton
            public ReadWriteThreadPools pools(AsyncFramework async, LocalMetadataBackendReporter reporter) {
                return pools.construct(async, reporter.newThreadPool());
            }

            @Provides
            @Singleton
            public Managed<Connection> connection(ManagedConnectionFactory connection) throws IOException {
                return connection.constructDefaultSettings(TEMPLATE_NAME, mappings());
            }

            @Override
            protected void configure() {
                bind(ManagedConnectionFactory.class).toInstance(connection);
                bind(key).toInstance(new ElasticsearchSuggestBackend(groups));
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
        return String.format("elasticsearch-suggest#%d", i);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String id;
        private String group;
        private Set<String> groups;
        private ManagedConnectionFactory connection;
        private ReadWriteThreadPools.Config pools;

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder group(String group) {
            this.group = group;
            return this;
        }

        public Builder group(Set<String> groups) {
            this.groups = groups;
            return this;
        }

        public Builder connection(ManagedConnectionFactory connection) {
            this.connection = connection;
            return this;
        }

        public Builder pools(ReadWriteThreadPools.Config pools) {
            this.pools = pools;
            return this;
        }

        public ElasticsearchSuggestModule build() {
            return new ElasticsearchSuggestModule(id, group, groups, connection, pools);
        }
    }
}
