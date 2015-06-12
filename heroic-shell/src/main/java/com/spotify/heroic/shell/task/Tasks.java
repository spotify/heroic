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

package com.spotify.heroic.shell.task;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.HeroicConfig;
import com.spotify.heroic.HeroicCore;
import com.spotify.heroic.HeroicProfile;
import com.spotify.heroic.elasticsearch.ManagedConnectionFactory;
import com.spotify.heroic.elasticsearch.TransportClientSetup;
import com.spotify.heroic.elasticsearch.index.SingleIndexMapping;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.metadata.MetadataManagerModule;
import com.spotify.heroic.metadata.MetadataModule;
import com.spotify.heroic.metadata.elasticsearch.ElasticsearchMetadataModule;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.RangeFilter;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.suggest.SuggestManagerModule;
import com.spotify.heroic.suggest.SuggestModule;
import com.spotify.heroic.suggest.elasticsearch.ElasticsearchSuggestModule;

public final class Tasks {
    public static Filter setupFilter(FilterFactory filters, QueryParser parser, QueryParams params) {
        final List<String> query = params.getQuery();

        if (query.isEmpty())
            return filters.t();

        return parser.parseFilter(StringUtils.join(query, " "));
    }

    public abstract static class QueryParamsBase extends AbstractShellTaskParams implements QueryParams {
        private final DateRange defaultDateRange;

        public QueryParamsBase() {
            final long now = System.currentTimeMillis();
            final long start = now - TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS);
            this.defaultDateRange = new DateRange(start, now);
        }

        @Override
        public DateRange getRange() {
            return defaultDateRange;
        }
    }

    public static RangeFilter setupRangeFilter(FilterFactory filters, QueryParser parser, QueryParams params) {
        final Filter filter = setupFilter(filters, parser, params);
        return new RangeFilter(filter, params.getRange(), params.getLimit());
    }

    public static void standaloneElasticsearchConfig(HeroicCore.Builder builder, ElasticSearchParams params) {
        final List<String> seeds = Arrays.asList(StringUtils.split(params.getSeeds(), ','));

        final String clusterName = params.getClusterName();

        builder.profile(new HeroicProfile() {
            @Override
            public HeroicConfig build() throws Exception {
                // @formatter:off

                final TransportClientSetup clientSetup = TransportClientSetup.builder()
                    .clusterName(clusterName)
                    .seeds(seeds)
                .build();

                return HeroicConfig.builder()
                        .metadata(
                            MetadataManagerModule.builder()
                            .backends(
                                ImmutableList.<MetadataModule>of(
                                    ElasticsearchMetadataModule.builder()
                                    .connection(setupConnection(clientSetup, "metadata"))
                                    .writesPerSecond(0d)
                                    .build()
                                )
                            ).build()
                        )
                        .suggest(
                            SuggestManagerModule.builder()
                            .backends(
                                ImmutableList.<SuggestModule>of(
                                    ElasticsearchSuggestModule.builder()
                                    .connection(setupConnection(clientSetup, "suggest"))
                                    .writesPerSecond(0d)
                                    .build()
                                )
                            )
                            .build()
                        )

                .build();
                // @formatter:on
            }

            private ManagedConnectionFactory setupConnection(TransportClientSetup clientSetup, final String index) {
                // @formatter:off
                return ManagedConnectionFactory.builder()
                    .clientSetup(clientSetup)
                    .index(SingleIndexMapping.builder().index(index).build())
                    .build();
                // @formatter:on
            }

            @Override
            public String description() {
                return "load metadata form a file";
            }
        });
    }

    public static interface QueryParams {
        public List<String> getQuery();

        public DateRange getRange();

        public int getLimit();
    }

    public static interface ElasticSearchParams {
        public String getSeeds();

        public String getClusterName();
    }
}