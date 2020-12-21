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
import dagger.Provides;
import java.util.Optional;
import javax.inject.Named;

// TODO probably move this to bigtable module since that's the only implementation...?
// or maybe not, precisely for that very reason.
@ModuleId("connectionsettings")
@dagger.Module
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
    public static MetricsConnectionSettings createDefault() {
        return new MetricsConnectionSettingsModule();
    }

    @Provides
    @Named("maxWriteBatchSize")
    public Integer maxWriteBatchSize() {
        return super.maxWriteBatchSizeImpl();
    }

    @Provides
    @Named("mutateRpcTimeoutMs")
    public Integer mutateRpcTimeoutMs() {
        return super.mutateRpcTimeoutMsImpl();
    }

    @Provides
    @Named("readRowsRpcTimeoutMs")
    public Integer readRowsRpcTimeoutMs() {
        return super.readRowsRpcTimeoutMsImpl();
    }

    @Provides
    @Named("shortRpcTimeoutMs")
    public Integer shortRpcTimeoutMs() {
        return super.shortRpcTimeoutMsImpl();
    }

    @Provides
    @Named("maxScanTimeoutRetries")
    public Integer maxScanTimeoutRetries() {
        return super.maxScanTimeoutRetriesImpl();
    }

    @Provides
    @Named("maxElapsedBackoffMs")
    public Integer maxElapsedBackoffMs() {
        return super.maxElapsedBackoffMsImpl();
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
