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

import static com.spotify.heroic.metric.consts.ApiQueryConsts.DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS;
import static com.spotify.heroic.metric.consts.ApiQueryConsts.DEFAULT_MAX_SCAN_TIMEOUT_RETRIES;
import static com.spotify.heroic.metric.consts.ApiQueryConsts.DEFAULT_MUTATE_RPC_TIMEOUT_MS;
import static com.spotify.heroic.metric.consts.ApiQueryConsts.DEFAULT_READ_ROWS_RPC_TIMEOUT_MS;
import static com.spotify.heroic.metric.consts.ApiQueryConsts.DEFAULT_SHORT_RPC_TIMEOUT_MS;

import java.util.Optional;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class MetricsConnectionSettings {
    /* default number of Cells for each batch mutation */
    public static final int DEFAULT_MUTATION_BATCH_SIZE = 1_000;

    /* maximum possible number of Cells for each batch mutation */
    public static final int MAX_MUTATION_BATCH_SIZE = 100_000;

    /* minimum possible number of Cells supported for each batch mutation */
    public static final int MIN_MUTATION_BATCH_SIZE = 10;

    protected Integer mutateRpcTimeoutMs;
    protected Integer readRowsRpcTimeoutMs;
    protected Integer shortRpcTimeoutMs;
    protected Integer maxScanTimeoutRetries;
    protected Integer maxElapsedBackoffMs;
    protected Integer maxWriteBatchSize;

    protected MetricsConnectionSettings() {
        this(Optional.of(MAX_MUTATION_BATCH_SIZE), Optional.of(DEFAULT_MUTATE_RPC_TIMEOUT_MS),
                Optional.of(DEFAULT_READ_ROWS_RPC_TIMEOUT_MS),
                Optional.of(DEFAULT_SHORT_RPC_TIMEOUT_MS),
                Optional.of(DEFAULT_MAX_SCAN_TIMEOUT_RETRIES),
                Optional.of(DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS));
    }

    public static MetricsConnectionSettings createDefault() {
        return new MetricsConnectionSettings();
    }

    public MetricsConnectionSettings(
            Optional<Integer> maxWriteBatchSize,
            Optional<Integer> mutateRpcTimeoutMs,
            Optional<Integer> readRowsRpcTimeoutMs,
            Optional<Integer> shortRpcTimeoutMs,
            Optional<Integer> maxScanTimeoutRetries,
            Optional<Integer> maxElapsedBackoffMs) {
        // Basically make sure that maxWriteBatchSize, if set, is sane
        int maxWriteBatch = maxWriteBatchSize.orElse(DEFAULT_MUTATION_BATCH_SIZE);
        maxWriteBatch = Math.max(MIN_MUTATION_BATCH_SIZE, maxWriteBatch);
        maxWriteBatch = Math.min(MAX_MUTATION_BATCH_SIZE, maxWriteBatch);

        this.maxWriteBatchSize = maxWriteBatch;

        this.mutateRpcTimeoutMs = mutateRpcTimeoutMs.orElse(DEFAULT_MUTATE_RPC_TIMEOUT_MS);
        this.readRowsRpcTimeoutMs = readRowsRpcTimeoutMs.
                orElse(DEFAULT_READ_ROWS_RPC_TIMEOUT_MS);
        this.shortRpcTimeoutMs = shortRpcTimeoutMs.orElse(DEFAULT_SHORT_RPC_TIMEOUT_MS);
        this.maxScanTimeoutRetries = maxScanTimeoutRetries.
                orElse(DEFAULT_MAX_SCAN_TIMEOUT_RETRIES);
        this.maxElapsedBackoffMs =
                maxElapsedBackoffMs.orElse(DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS);
    }

    public Integer getMaxWriteBatchSize() {
        return maxWriteBatchSize;
    }

    public Integer getMutateRpcTimeoutMs() {
        return mutateRpcTimeoutMs;
    }

    public Integer getReadRowsRpcTimeoutMs() {
        return readRowsRpcTimeoutMs;
    }

    public Integer getShortRpcTimeoutMs() {
        return shortRpcTimeoutMs;
    }

    public Integer getMaxScanTimeoutRetries() {
        return maxScanTimeoutRetries;
    }

    public Integer getMaxElapsedBackoffMs() {
        return maxElapsedBackoffMs;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.MULTI_LINE_STYLE)
                .append("maxWriteBatchSize", maxWriteBatchSize)
                .append("mutateRpcTimeoutMs", mutateRpcTimeoutMs)
                .append("readRowsRpcTimeoutMs", readRowsRpcTimeoutMs)
                .append("shortRpcTimeoutMs", shortRpcTimeoutMs)
                .append("maxScanTimeoutRetries", maxScanTimeoutRetries)
                .append("maxElapsedBackoffMs", maxElapsedBackoffMs)
                .toString();
    }
}
