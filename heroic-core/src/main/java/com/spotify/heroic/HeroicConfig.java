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

package com.spotify.heroic;

import java.util.List;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.aggregationcache.AggregationCacheModule;
import com.spotify.heroic.cluster.ClusterManagerModule;
import com.spotify.heroic.consumer.ConsumerModule;
import com.spotify.heroic.httpclient.HttpClientManagerModule;
import com.spotify.heroic.ingestion.IngestionModule;
import com.spotify.heroic.metadata.MetadataManagerModule;
import com.spotify.heroic.metric.MetricManagerModule;
import com.spotify.heroic.suggest.SuggestManagerModule;

@RequiredArgsConstructor
@Data
public class HeroicConfig {
    public static final String DEFAULT_HOST = "0.0.0.0";
    public static final int DEFAULT_PORT = 8080;
    public static final String DEFAULT_REFRESH_CLUSTER_SCHEDULE = "0 */5 * * * ?";
    public static final List<ConsumerModule> DEFAULT_CONSUMERS = ImmutableList.of();

    private final String host;
    private final int port;
    private final String refreshClusterSchedule;

    private final ClusterManagerModule cluster;
    private final MetricManagerModule metric;
    private final MetadataManagerModule metadata;
    private final SuggestManagerModule suggest;
    private final AggregationCacheModule cache;
    private final HttpClientManagerModule client;
    private final IngestionModule ingestion;
    private final List<ConsumerModule> consumers;

    @JsonCreator
    public HeroicConfig(@JsonProperty("host") String host, @JsonProperty("port") Integer port,
            @JsonProperty("refreshClusterSchedule") String refreshClusterSchedule,
            @JsonProperty("cluster") ClusterManagerModule cluster,
            @JsonProperty("metrics") MetricManagerModule metrics,
            @JsonProperty("metadata") MetadataManagerModule metadata,
            @JsonProperty("suggest") SuggestManagerModule suggest, @JsonProperty("cache") AggregationCacheModule cache,
            @JsonProperty("client") HttpClientManagerModule client,
            @JsonProperty("ingestion") IngestionModule ingestion,
            @JsonProperty("consumers") List<ConsumerModule> consumers) {
        this.host = Optional.fromNullable(host).or(DEFAULT_HOST);
        this.port = Optional.fromNullable(port).or(DEFAULT_PORT);
        this.refreshClusterSchedule = Optional.fromNullable(refreshClusterSchedule)
                .or(DEFAULT_REFRESH_CLUSTER_SCHEDULE);
        this.cluster = Optional.fromNullable(cluster).or(ClusterManagerModule.defaultSupplier());
        this.metric = Optional.fromNullable(metrics).or(MetricManagerModule.defaultSupplier());
        this.metadata = Optional.fromNullable(metadata).or(MetadataManagerModule.defaultSupplier());
        this.suggest = Optional.fromNullable(suggest).or(SuggestManagerModule.defaultSupplier());
        this.client = Optional.fromNullable(client).or(HttpClientManagerModule.defaultSupplier());
        this.cache = Optional.fromNullable(cache).or(AggregationCacheModule.defaultSupplier());
        this.ingestion = Optional.fromNullable(ingestion).or(IngestionModule.defaultSupplier());
        this.consumers = Optional.fromNullable(consumers).or(DEFAULT_CONSUMERS);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String host;
        private Integer port;
        private String refreshClusterSchedule;
        private ClusterManagerModule cluster;
        private MetricManagerModule metric;
        private MetadataManagerModule metadata;
        private SuggestManagerModule suggest;
        private AggregationCacheModule cache;
        private HttpClientManagerModule client;
        private IngestionModule ingestion;
        private List<ConsumerModule> consumers;

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(Integer port) {
            this.port = port;
            return this;
        }

        public Builder refreshClusterSchedule(String refreshClusterSchedule) {
            this.refreshClusterSchedule = refreshClusterSchedule;
            return this;
        }

        public Builder cluster(ClusterManagerModule cluster) {
            this.cluster = cluster;
            return this;
        }

        public Builder metric(MetricManagerModule metric) {
            this.metric = metric;
            return this;
        }

        public Builder metadata(MetadataManagerModule metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder suggest(SuggestManagerModule suggest) {
            this.suggest = suggest;
            return this;
        }

        public Builder cache(AggregationCacheModule cache) {
            this.cache = cache;
            return this;
        }

        public Builder client(HttpClientManagerModule client) {
            this.client = client;
            return this;
        }

        public Builder ingestion(IngestionModule ingestion) {
            this.ingestion = ingestion;
            return this;
        }

        public Builder consumers(List<ConsumerModule> consumers) {
            this.consumers = consumers;
            return this;
        }

        public HeroicConfig build() {
            return new HeroicConfig(host, port, refreshClusterSchedule, cluster, metric, metadata, suggest, cache,
                    client, ingestion, consumers);
        }
    }
}
