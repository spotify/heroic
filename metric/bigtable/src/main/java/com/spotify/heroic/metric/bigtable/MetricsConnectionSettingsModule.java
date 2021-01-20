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

package com.spotify.heroic.metric.bigtable;

import com.spotify.heroic.common.ModuleId;
import com.spotify.heroic.metric.MetricsConnectionSettings;
import dagger.Module;
import dagger.Provides;
import java.util.Optional;
import javax.inject.Named;

/**
 * Simple POD class that exposes MetricsConnectionSettings via Dagger's module
 * functionality.
 */
@Module
@ModuleId("bigtable")
public class MetricsConnectionSettingsModule extends MetricsConnectionSettings {
    public MetricsConnectionSettingsModule(
            Optional<Integer> maxWriteBatchSize,
            Optional<Integer> mutateRpcTimeoutMs,
            Optional<Integer> readRowsRpcTimeoutMs,
            Optional<Integer> shortRpcTimeoutMs,
            Optional<Integer> maxScanTimeoutRetries,
            Optional<Integer> maxElapsedBackoffMs
    ) {
        super(maxWriteBatchSize, mutateRpcTimeoutMs, readRowsRpcTimeoutMs, shortRpcTimeoutMs,
                maxScanTimeoutRetries, maxElapsedBackoffMs);
    }

    public MetricsConnectionSettingsModule() {
        super();
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
        return super.mutateRpcTimeoutMs;
    }

    @Provides
    @Named("readRowsRpcTimeoutMs")
    public Integer readRowsRpcTimeoutMsProvides() {
        return super.readRowsRpcTimeoutMs;
    }

    @Provides
    @Named("shortRpcTimeoutMs")
    public Integer shortRpcTimeoutMsProvides() {
        return super.shortRpcTimeoutMs;
    }

    @Provides
    @Named("maxScanTimeoutRetries")
    public Integer maxScanTimeoutRetriesProvides() {
        return super.maxScanTimeoutRetries;
    }

    @Provides
    @Named("maxElapsedBackoffMs")
    public Integer maxElapsedBackoffMsProvides() {
        return super.maxElapsedBackoffMs;
    }
}
