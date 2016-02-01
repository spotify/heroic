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

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.ExtraParameters;
import com.spotify.heroic.HeroicConfig;
import com.spotify.heroic.ParameterSpecification;
import com.spotify.heroic.analytics.bigtable.BigtableAnalyticsModule;
import com.spotify.heroic.metric.bigtable.credentials.ComputeEngineCredentialsBuilder;
import com.spotify.heroic.metric.bigtable.credentials.JsonCredentialsBuilder;
import com.spotify.heroic.metric.bigtable.credentials.ServiceAccountCredentialsBuilder;

import java.nio.file.Paths;
import java.util.List;

public class BigtableAnalyticsProfile extends HeroicProfileBase {
    public static final String DEFAULT_CREDENTIALS = "json";

    @Override
    public HeroicConfig.Builder build(final ExtraParameters params) throws Exception {
        final BigtableAnalyticsModule.Builder module = BigtableAnalyticsModule.builder();

        params.get("bigtable-analytics.project").map(module::project);
        params.get("bigtable-analytics.zone").map(module::zone);
        params.get("bigtable-analytics.cluster").map(module::cluster);

        final String credentials =
                params.get("bigtable-analytics.credential").orElse(DEFAULT_CREDENTIALS);

        switch (credentials) {
        case "json":
            final JsonCredentialsBuilder.Builder j = JsonCredentialsBuilder.builder();
            params.get("bigtable-analytics.json").map(Paths::get).ifPresent(j::path);
            module.credentials(j.build());
            break;
        case "service-account":
            final ServiceAccountCredentialsBuilder.Builder sa =
                    ServiceAccountCredentialsBuilder.builder();
            params.get("bigtable-analytics.serviceAccount").ifPresent(sa::serviceAccount);
            params.get("bigtable-analytics.keyFile").ifPresent(sa::keyFile);
            module.credentials(sa.build());
            break;
        case "compute-engine":
            module.credentials(new ComputeEngineCredentialsBuilder());
            break;
        default:
            throw new IllegalArgumentException(
                    "bigtable-analytics.credentials: invalid value: " + credentials);
        }

        return HeroicConfig.builder().analytics(module);
    }

    @Override
    public String description() {
        return "Configures an analytics backend for Bigtable";
    }

    @Override
    public List<ParameterSpecification> options() {
        // @formatter:off
        return ImmutableList.of(
            parameter("bigtable-analytics.configure", "If set, will cause the cluster to be automatically configured"),
            parameter("bigtable-analytics.project", "Bigtable project to use", "<project>"),
            parameter("bigtable-analytics.zone", "Bigtable zone to use", "<zone>"),
            parameter("bigtable-analytics.cluster", "Bigtable cluster to use", "<cluster>"),
            parameter("bigtable-analytics.credentials", "Credentials implementation to use, must be one of: compute-engine (default), json, service-account", "<credentials>"),
            parameter("bigtable-analytics.json", "Json file to use when using json credentials", "<file>"),
            parameter("bigtable-analytics.serviceAccount", "Service account to use when using service-account credentials", "<account>"),
            parameter("bigtable-analytics.keyFile", "Key file to use when using service-account credentials", "<file>")
        );
        // @formatter:on
    }
}
