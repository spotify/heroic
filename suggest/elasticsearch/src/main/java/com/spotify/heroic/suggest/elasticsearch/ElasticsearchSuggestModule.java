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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.inject.Singleton;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.spotify.heroic.concurrrency.ReadWriteThreadPools;
import com.spotify.heroic.elasticsearch.Connection;
import com.spotify.heroic.elasticsearch.ElasticsearchUtils;
import com.spotify.heroic.elasticsearch.ManagedConnectionFactory;
import com.spotify.heroic.statistics.LocalMetadataBackendReporter;
import com.spotify.heroic.statistics.LocalMetadataManagerReporter;
import com.spotify.heroic.suggest.SuggestBackend;
import com.spotify.heroic.suggest.SuggestModule;
import com.spotify.heroic.utils.GroupedUtils;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.Managed;

public final class ElasticsearchSuggestModule implements SuggestModule {
    public static final String DEFAULT_GROUP = "elasticsearch";
    public static final String TEMPLATE_NAME = "heroic-suggest";

    private final String id;
    private final Set<String> groups;
    private final ReadWriteThreadPools.Config pools;
    private final ManagedConnectionFactory connection;

    @JsonCreator
    public ElasticsearchSuggestModule(@JsonProperty("id") String id, @JsonProperty("group") String group,
            @JsonProperty("groups") Set<String> groups, @JsonProperty("pools") ReadWriteThreadPools.Config pools,
            @JsonProperty("connection") ManagedConnectionFactory connection) {
        this.id = id;
        this.groups = GroupedUtils.groups(group, groups, DEFAULT_GROUP);
        this.pools = Optional.fromNullable(pools).or(ReadWriteThreadPools.Config.provideDefault());
        this.connection = Optional.fromNullable(connection).or(ManagedConnectionFactory.provideDefault());
    }

    private Map<String, XContentBuilder> mappings() throws IOException {
        final Map<String, XContentBuilder> mappings = new HashMap<>();
        mappings.put("tag", buildTagMapping());
        mappings.put("series", buildSeriesMapping());
        return mappings;
    }

    private XContentBuilder buildSeriesMapping() throws IOException {
        final XContentBuilder b = XContentFactory.jsonBuilder();

        // @formatter:off
        b.startObject();
          b.startObject(ElasticsearchUtils.TYPE_SERIES);
            b.startObject("properties");
              b.startObject(ElasticsearchUtils.KEY);
                b.field("type", "string");
                b.startObject("fields");
                  b.startObject("raw");
                    b.field("type", "string");
                    b.field("index", "not_analyzed");
                    b.field("doc_values", true);
                  b.endObject();
                b.endObject();
              b.endObject();

              b.startObject(ElasticsearchUtils.TAGS);
                b.field("type", "nested");
                b.startObject("properties");
                  b.startObject(ElasticsearchUtils.TAGS_KEY);
                    b.field("type", "string");
                    b.startObject("fields");
                      b.startObject("raw");
                        b.field("type", "string");
                        b.field("index", "not_analyzed");
                        b.field("doc_values", true);
                      b.endObject();
                    b.endObject();
                  b.endObject();

                  b.startObject(ElasticsearchUtils.TAGS_VALUE);
                    b.field("type", "string");
                    b.startObject("fields");
                      b.startObject("raw");
                        b.field("type", "string");
                        b.field("index", "not_analyzed");
                        b.field("doc_values", true);
                      b.endObject();
                    b.endObject();
                  b.endObject();
                b.endObject();
              b.endObject();
              /* end tags */
            b.endObject();
          b.endObject();
        b.endObject();
        // @formatter:on

        return b;
    }

    private XContentBuilder buildTagMapping() throws IOException {
        final XContentBuilder b = XContentFactory.jsonBuilder();

        // @formatter:off
        b.startObject();
          b.startObject(ElasticsearchUtils.TYPE_TAG);
            b.startObject("properties");

              b.startObject(ElasticsearchUtils.TAG_KEY);
                b.field("type", "string");
                b.startObject("fields");
                  b.startObject("raw");
                    b.field("type", "string");
                    b.field("index", "not_analyzed");
                    b.field("doc_values", true);
                  b.endObject();
                b.endObject();
              b.endObject();

              b.startObject(ElasticsearchUtils.TAG_VALUE);
                b.field("type", "string");
                b.startObject("fields");
                  b.startObject("raw");
                    b.field("type", "string");
                    b.field("index", "not_analyzed");
                    b.field("doc_values", true);
                  b.endObject();
                b.endObject();
              b.endObject();

              b.startObject(ElasticsearchUtils.TAG_KV);
                b.field("type", "string");
                b.field("index", "not_analyzed");
                b.field("doc_values", true);
              b.endObject();

              // series is solely used for filtering, nothing should be stored or analyzed.
              b.startObject(ElasticsearchUtils.TAG_SERIES);
                b.field("type", "nested");
                b.field("include_in_parent", true);
                b.startObject("properties");
                  // used for cardinality aggregation.
                  b.startObject(ElasticsearchUtils.ID);
                    b.field("type", "string");
                    b.field("index", "not_analyzed");
                    b.field("doc_values", true);
                  b.endObject();

                  b.startObject(ElasticsearchUtils.KEY);
                    b.field("type", "string");
                    b.field("index", "not_analyzed");
                    b.field("doc_values", true);
                    b.field("store", false);
                  b.endObject();

                  b.startObject(ElasticsearchUtils.TAGS);
                    b.field("type", "nested");
                    b.startObject("properties");
                      b.startObject(ElasticsearchUtils.TAGS_KEY);
                        b.field("type", "string");
                        b.startObject("fields");
                          b.startObject("raw");
                            b.field("type", "string");
                            b.field("index", "not_analyzed");
                            b.field("doc_values", true);
                          b.endObject();
                        b.endObject();
                      b.endObject();

                      b.startObject(ElasticsearchUtils.TAGS_VALUE);
                        b.field("type", "string");
                        b.startObject("fields");
                          b.startObject("raw");
                            b.field("type", "string");
                            b.field("index", "not_analyzed");
                            b.field("doc_values", true);
                          b.endObject();
                        b.endObject();
                      b.endObject();
                    b.endObject();
                  b.endObject();
                  /* end tags */
                b.endObject();
              b.endObject();
            b.endObject();
          b.endObject();
        b.endObject();
        // @formatter:on

        return b;
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
                return connection.construct(TEMPLATE_NAME, mappings());
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
}
