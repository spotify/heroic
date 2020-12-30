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

package com.spotify.heroic.metric;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.common.ModuleId;
import dagger.Module;
import dagger.Provides;
import java.util.Optional;
import javax.inject.Named;

// TODO probably move this to bigtable module since that's the only implementation...?
// or maybe not, precisely for that very reason.
@Module
@ModuleId("bigtable")
public class MetricsConnectionSettingsModule extends MetricsConnectionSettings {
    @JsonCreator
    public MetricsConnectionSettingsModule(
            @JsonProperty("maxWriteBatchSize") Optional<Integer> maxWriteBatchSize,
            @JsonProperty("mutateRpcTimeoutMs") Optional<Integer> mutateRpcTimeoutMs,
            @JsonProperty("readRowsRpcTimeoutMs") Optional<Integer> readRowsRpcTimeoutMs,
            @JsonProperty("shortRpcTimeoutMs") Optional<Integer> shortRpcTimeoutMs,
            @JsonProperty("maxScanTimeoutRetries") Optional<Integer> maxScanTimeoutRetries,
            @JsonProperty("maxElapsedBackoffMs") Optional<Integer> maxElapsedBackoffMs
    ) {
        super(maxWriteBatchSize, mutateRpcTimeoutMs, readRowsRpcTimeoutMs, shortRpcTimeoutMs,
                maxScanTimeoutRetries, maxElapsedBackoffMs);
    }

    @JsonCreator
    public MetricsConnectionSettingsModule() {
        super();
    }

    @JsonCreator
    public static MetricsConnectionSettingsModule createDefault() {
        return new MetricsConnectionSettingsModule();
    }

    @Provides
    @Named("metricsConnectionSettings")
    public MetricsConnectionSettings metricsConnectionSettingsProvides() {
        return this;
    }

    @Provides
    @Named("maxWriteBatchSize")
    public Integer maxWriteBatchSizeProvides() {
        return super.getMaxWriteBatchSize();
    }

    @Provides
    @Named("mutateRpcTimeoutMs")
    public Integer mutateRpcTimeoutMsProvides() {
        return super.getMutateRpcTimeoutMs();
    }

    @Provides
    @Named("readRowsRpcTimeoutMs")
    public Integer readRowsRpcTimeoutMsProvides() {
        return super.getReadRowsRpcTimeoutMs();
    }

    @Provides
    @Named("shortRpcTimeoutMs")
    public Integer shortRpcTimeoutMsProvides() {
        return super.getShortRpcTimeoutMs();
    }

    @Provides
    @Named("maxScanTimeoutRetries")
    public Integer maxScanTimeoutRetriesProvides() {
        return super.getMaxScanTimeoutRetries();
    }

    @Provides
    @Named("maxElapsedBackoffMs")
    public Integer maxElapsedBackoffMsProvides() {
        return super.getMaxElapsedBackoffMs();
    }
}
