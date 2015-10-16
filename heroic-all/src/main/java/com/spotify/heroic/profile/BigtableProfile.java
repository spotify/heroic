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

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.HeroicConfig;
import com.spotify.heroic.HeroicParameters;
import com.spotify.heroic.HeroicProfile;
import com.spotify.heroic.metric.MetricManagerModule;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.metric.bigtable.BigtableMetricModule;
import com.spotify.heroic.metric.bigtable.credentials.ServiceAccountCredentialsBuilder;

public class BigtableProfile extends HeroicProfileBase {
    @Override
    public HeroicConfig.Builder build(final HeroicParameters params) throws Exception {
        final BigtableMetricModule.Builder module = BigtableMetricModule.builder();

        params.get("bigtable.project").map(module::project);
        params.get("bigtable.zone").map(module::zone);
        params.get("bigtable.cluster").map(module::cluster);

        final ServiceAccountCredentialsBuilder.Builder credentials = ServiceAccountCredentialsBuilder.builder();

        params.get("bigtable.serviceAccount").map(credentials::serviceAccount);
        params.get("bigtable.keyFile").map(credentials::keyFile);

        module.credentials(credentials.build());

        // @formatter:off
        return HeroicConfig.builder()
            .metric(
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
    public List<Option> options() {
        // @formatter:off
        return ImmutableList.of(
            HeroicProfile.option("bigtable.configure", "If set, will cause the cluster to be automatically configured"),
            HeroicProfile.option("bigtable.project", "Bigtable project to use", "<project>"),
            HeroicProfile.option("bigtable.zone", "Bigtable zone to use", "<zone>"),
            HeroicProfile.option("bigtable.cluster", "Bigtable cluster to use", "<cluster>"),
            HeroicProfile.option("bigtable.serviceAccount", "Bigtable cluster to use", "<cluster>"),
            HeroicProfile.option("bigtable.keyFile", "Bigtable cluster to use", "<cluster>")
        );
        // @formatter:on
    }
}
