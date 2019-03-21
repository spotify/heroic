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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.profile.BigtableAnalyticsProfile;
import com.spotify.heroic.profile.BigtableProfile;
import com.spotify.heroic.profile.CassandraProfile;
import com.spotify.heroic.profile.ClusterProfile;
import com.spotify.heroic.profile.CollectdConsumerProfile;
import com.spotify.heroic.profile.ElasticsearchMetadataProfile;
import com.spotify.heroic.profile.ElasticsearchSuggestProfile;
import com.spotify.heroic.profile.KafkaConsumerProfile;
import com.spotify.heroic.profile.MemoryCacheProfile;
import com.spotify.heroic.profile.MemoryProfile;
import com.spotify.heroic.profile.PubSubConsumerProfile;
import com.spotify.heroic.profile.QueryLoggingProfile;
import com.spotify.heroic.profile.WebProfile;

import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

public class HeroicModules {
    // @formatter:off
    public static final List<HeroicModule> ALL_MODULES = ImmutableList.of(
        new com.spotify.heroic.conditionalfeatures.Module(),
        new com.spotify.heroic.requestcondition.Module(),

        new com.spotify.heroic.metric.datastax.Module(),
        new com.spotify.heroic.metric.memory.Module(),

        new com.spotify.heroic.analytics.bigtable.Module(),
        new com.spotify.heroic.metric.bigtable.Module(),

        new com.spotify.heroic.metadata.elasticsearch.Module(),
        new com.spotify.heroic.metadata.memory.Module(),

        new com.spotify.heroic.suggest.elasticsearch.Module(),
        new com.spotify.heroic.suggest.memory.Module(),

        new com.spotify.heroic.cluster.discovery.simple.Module(),

        new com.spotify.heroic.aggregation.simple.Module(),
        new com.spotify.heroic.aggregation.cardinality.Module(),

        new com.spotify.heroic.consumer.kafka.Module(),
        new com.spotify.heroic.consumer.pubsub.Module(),
        new com.spotify.heroic.consumer.collectd.Module(),

        new com.spotify.heroic.rpc.grpc.Module(),
        new com.spotify.heroic.rpc.jvm.Module(),

        new com.spotify.heroic.statistics.semantic.Module(),

        new com.spotify.heroic.querylogging.Module()
    );

    public static final Map<String, HeroicProfile> PROFILES = ImmutableMap.<String,
            HeroicProfile>builder()
        .put("memory", new MemoryProfile())
        .put("cassandra", new CassandraProfile())
        .put("elasticsearch-metadata", new ElasticsearchMetadataProfile())
        .put("elasticsearch-suggest", new ElasticsearchSuggestProfile())
        .put("kafka-consumer", new KafkaConsumerProfile())
        .put("pubsub", new PubSubConsumerProfile())
        .put("bigtable", new BigtableProfile())
        .put("bigtable-analytics", new BigtableAnalyticsProfile())
        .put("cluster", new ClusterProfile())
        .put("collectd", new CollectdConsumerProfile())
        .put("memory-cache", new MemoryCacheProfile())
        .put("web", new WebProfile())
        .put("query-logging", new QueryLoggingProfile())
    .build();
    // @formatter:on

    public static void printAllUsage(final PrintWriter out, final String option) {
        out.println("Available Extra Parameters:");

        ExtraParameters.CONFIGURE.printHelp(out, "  ", 80);

        for (final HeroicModule m : HeroicCore.builtinModules()) {
            for (final ParameterSpecification p : m.parameters()) {
                p.printHelp(out, "  ", 80);
            }
        }

        for (final HeroicModule m : ALL_MODULES) {
            for (final ParameterSpecification p : m.parameters()) {
                p.printHelp(out, "  ", 80);
            }
        }

        out.println();

        out.println(String.format("Available Profiles (activate with: %s <profile>):", option));

        for (final Map.Entry<String, HeroicProfile> entry : PROFILES.entrySet()) {
            ParameterSpecification.printWrapped(out, "  ", 80,
                entry.getKey() + " - " + entry.getValue().description());

            for (final ParameterSpecification o : entry.getValue().options()) {
                o.printHelp(out, "    ", 80, entry.getValue().scope());
            }

            out.println();
        }

        out.flush();
    }

    public static void printAllUsage(final PrintStream out, final String option) {
        final PrintWriter o = new PrintWriter(new OutputStreamWriter(out, Charsets.UTF_8));

        try {
            printAllUsage(o, option);
        } finally {
            o.flush();
        }
    }
}
