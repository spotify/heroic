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

package com.spotify.heroic.profile;

import static com.spotify.heroic.ParameterSpecification.parameter;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.ExtraParameters;
import com.spotify.heroic.HeroicConfig;
import com.spotify.heroic.ParameterSpecification;
import com.spotify.heroic.metric.MetricManagerModule;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.metric.datastax.DatastaxMetricModule;
import com.spotify.heroic.metric.datastax.schema.SchemaModule;
import com.spotify.heroic.metric.datastax.schema.ng.NextGenSchemaModule;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

public class CassandraProfile extends HeroicProfileBase {
    private static final Splitter splitter = Splitter.on(',').trimResults();

    private static final Map<String, Callable<SchemaModule>> types = new HashMap<>();

    {
        types.put("ng", () -> NextGenSchemaModule.builder().build());
    }

    @Override
    public HeroicConfig.Builder build(final ExtraParameters params) throws Exception {
        final DatastaxMetricModule.Builder module = DatastaxMetricModule.builder();

        module.configure(params.contains("configure"));

        final Optional<String> type = params.get("type");

        if (type.isPresent()) {
            final Callable<SchemaModule> builder;

            if ((builder = types.get(type.get())) == null) {
                throw new IllegalArgumentException("Unknown type: " + type.get());
            }

            module.schema(builder.call());
        } else {
            module.schema(NextGenSchemaModule.builder().build());
        }

        module.seeds(params
          .get("seeds")
          .map(s -> ImmutableSet.copyOf(splitter.split(s)))
          .orElseGet(() -> ImmutableSet.of("localhost")));

        params.getInteger("fetchSize").ifPresent(module::fetchSize);
        params.getDuration("readTimeout").ifPresent(module::readTimeout);
        params
          .get("consistencyLevel")
          .ifPresent(module::consistencyLevel);
        params
          .get("retryPolicy")
          .ifPresent(policy -> this.setRetryPolicy(module, policy, params));

        // @formatter:off
        return HeroicConfig.builder()
          .metrics(
            MetricManagerModule.builder()
              .backends(ImmutableList.<MetricModule>of(
                module.build()
              ))
          );
        // @formatter:on
    }

    @Override
    public Optional<String> scope() {
        return Optional.of("cassandra");
    }

    @Override
    public String description() {
        return "Configures a metric backend for Cassandra";
    }

    private static final int DEFAULT_NUM_RETRIES = 10;
    private static final int DEFAULT_ROTATE_HOST = 2;

    private DatastaxMetricModule.Builder setRetryPolicy(
      DatastaxMetricModule.Builder module, final String policyName, final ExtraParameters params
    ) {
        if ("aggressive".equals(policyName)) {
            final int numRetries = params.getInteger("numRetries").orElse(DEFAULT_NUM_RETRIES);
            final int rotateHost = params.getInteger("rotateHost").orElse(DEFAULT_ROTATE_HOST);
            return module.aggressiveRetryPolicy(numRetries, rotateHost);
        }

        throw new IllegalArgumentException("Not a valid retry policy: " + policyName);
    }

    private static final Joiner parameters = Joiner.on(", ");

    @Override
    public List<ParameterSpecification> options() {
        // @formatter:off
        return ImmutableList.of(
            parameter("configure", "If set, will cause the cluster to be automatically " +
                    "configured"),
            parameter("type", "Type of backend to use, valid values are: " + parameters
                    .join(types.keySet()), "<type>"),
            parameter("seeds", "Seeds to use when configuring backend",
                    "<host>[:<port>][,..]"),
            parameter("fetchSize", "The number of results to fetch per batch", "<int>"),
            parameter("consistencyLevel", "The default consistency level to use",
                    parameters.join(DatastaxMetricModule.consistencyLevels())),
            parameter("retryPolicy", "The retry policy to use (useful when migrating " +
                    "data)", "aggressive"),
            parameter("numRetries", "The number of retries to attempt for the current " +
                    "retry policy", "<int>"),
            parameter("rotateHost", "The number of retries to attempt before rotating " +
                    "host for the current retry policy", "<int>")
        );
        // @formatter:on
    }
}
