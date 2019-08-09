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

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.ExtraParameters;
import com.spotify.heroic.HeroicConfig;
import com.spotify.heroic.ParameterSpecification;
import com.spotify.heroic.metric.MetricManagerModule;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.metric.bigtable.BigtableMetricModule;
import com.spotify.heroic.metric.bigtable.credentials.ComputeEngineCredentialsBuilder;
import com.spotify.heroic.metric.bigtable.credentials.DefaultCredentialsBuilder;
import com.spotify.heroic.metric.bigtable.credentials.JsonCredentialsBuilder;
import com.spotify.heroic.metric.bigtable.credentials.ServiceAccountCredentialsBuilder;

import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import static com.spotify.heroic.ParameterSpecification.parameter;

public class BigtableProfile extends HeroicProfileBase {
    public static final String DEFAULT_CREDENTIALS = "default";

    @Override
    public HeroicConfig.Builder build(final ExtraParameters params) throws Exception {
        final BigtableMetricModule.Builder module = BigtableMetricModule.builder();

        params.get("project").map(module::project);
        params.get("instance").map(module::instance);

        final String credentials = params.get("credential").orElse(DEFAULT_CREDENTIALS);

        switch (credentials) {
            case "json":
                final JsonCredentialsBuilder.Builder j = JsonCredentialsBuilder.builder();
                params.get("json").map(Paths::get).ifPresent(j::path);
                module.credentials(j.build());
                break;
            case "service-account":
                final ServiceAccountCredentialsBuilder.Builder sa =
                    ServiceAccountCredentialsBuilder.builder();
                params.get("serviceAccount").ifPresent(sa::serviceAccount);
                params.get("keyFile").ifPresent(sa::keyFile);
                module.credentials(sa.build());
                break;
            case "compute-engine":
                module.credentials(new ComputeEngineCredentialsBuilder());
                break;
            case "default":
                module.credentials(new DefaultCredentialsBuilder());
                break;
            default:
                throw new IllegalArgumentException("credentials: invalid value: " + credentials);
        }

        // @formatter:off
        return HeroicConfig.builder()
            .metrics(
                MetricManagerModule.builder()
                    .backends(ImmutableList.<MetricModule>of(module.build()))
            );
        // @formatter:on
    }

    @Override
    public String description() {
        return "Configures a metric backend for Bigtable";
    }

    @Override
    public Optional<String> scope() {
        return Optional.of("bigtable");
    }

    @Override
    public List<ParameterSpecification> options() {
        // @formatter:off
        return ImmutableList.of(
            parameter("configure", "If set, will cause the instance to be automatically " +
                    "configured"),
            parameter("project", "Bigtable project to use", "<project>"),
            parameter("instance", "Bigtable instance to use", "<instance>"),
            parameter("credentials", "Credentials implementation to use, must be one of:" +
                    " default, compute-engine, json, service-account", "<credentials>"),
            parameter("json", "Json file to use when using json credentials", "<file>"),
            parameter("serviceAccount", "Service account to use when using " +
                    "service-account credentials", "<account>"),
            parameter("keyFile", "Key file to use when using service-account " +
                "credentials", "<file>")
        );
        // @formatter:on
    }
}
