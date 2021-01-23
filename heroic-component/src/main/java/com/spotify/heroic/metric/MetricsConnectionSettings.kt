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
package com.spotify.heroic.metric

import com.spotify.heroic.metric.consts.ApiQueryConsts
import org.apache.commons.lang3.builder.ToStringBuilder
import org.apache.commons.lang3.builder.ToStringStyle
import java.util.*

open class MetricsConnectionSettings(
    maxWriteBatchSize: Optional<Int>,
    mutateRpcTimeoutMs: Optional<Int>,
    readRowsRpcTimeoutMs: Optional<Int>,
    shortRpcTimeoutMs: Optional<Int>,
    maxScanTimeoutRetries: Optional<Int>,
    maxElapsedBackoffMs: Optional<Int>
) {
    /**
     * See [com.spotify.heroic.metric.consts.ApiQueryConsts.DEFAULT_MUTATE_RPC_TIMEOUT_MS]
     */
    @JvmField
    var mutateRpcTimeoutMs: Int

    /**
     * See [com.spotify.heroic.metric.consts.ApiQueryConsts.DEFAULT_READ_ROWS_RPC_TIMEOUT_MS]
     */
    @JvmField
    var readRowsRpcTimeoutMs: Int

    /**
     * See [ApiQueryConsts.DEFAULT_SHORT_RPC_TIMEOUT_MS]
     */
    @JvmField
    var shortRpcTimeoutMs: Int

    /**
     * See [ApiQueryConsts.DEFAULT_MAX_SCAN_TIMEOUT_RETRIES]
     */
    @JvmField
    var maxScanTimeoutRetries: Int

    /**
     * See [ApiQueryConsts.DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS]
     */
    @JvmField
    var maxElapsedBackoffMs: Int

    /**
     * See [MetricsConnectionSettings.DEFAULT_MUTATION_BATCH_SIZE]
     */
    @JvmField
    var maxWriteBatchSize: Int

    protected constructor() : this(
        Optional.of<Int>(MAX_MUTATION_BATCH_SIZE),
        Optional.of<Int>(ApiQueryConsts.DEFAULT_MUTATE_RPC_TIMEOUT_MS),
        Optional.of<Int>(ApiQueryConsts.DEFAULT_READ_ROWS_RPC_TIMEOUT_MS),
        Optional.of<Int>(ApiQueryConsts.DEFAULT_SHORT_RPC_TIMEOUT_MS),
        Optional.of<Int>(ApiQueryConsts.DEFAULT_MAX_SCAN_TIMEOUT_RETRIES),
        Optional.of<Int>(ApiQueryConsts.DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS)
    ) {
    }

    override fun toString(): String {
        return ToStringBuilder(this, ToStringStyle.MULTI_LINE_STYLE)
            .append("maxWriteBatchSize", maxWriteBatchSize)
            .append("mutateRpcTimeoutMs", mutateRpcTimeoutMs)
            .append("readRowsRpcTimeoutMs", readRowsRpcTimeoutMs)
            .append("shortRpcTimeoutMs", shortRpcTimeoutMs)
            .append("maxScanTimeoutRetries", maxScanTimeoutRetries)
            .append("maxElapsedBackoffMs", maxElapsedBackoffMs)
            .toString()
    }

    companion object {
        /* default number of Cells for each batch mutation */
        const val DEFAULT_MUTATION_BATCH_SIZE = 1000

        /* maximum possible number of Cells for each batch mutation */
        const val MAX_MUTATION_BATCH_SIZE = 100000

        /* minimum possible number of Cells supported for each batch mutation */
        const val MIN_MUTATION_BATCH_SIZE = 10
        @JvmStatic
        fun createDefault(): MetricsConnectionSettings {
            return MetricsConnectionSettings()
        }
    }

    init {
        // Basically make sure that maxWriteBatchSize, if set, is sane
        var maxWriteBatch = maxWriteBatchSize.orElse(DEFAULT_MUTATION_BATCH_SIZE)
        maxWriteBatch = maxWriteBatch.coerceAtLeast(MIN_MUTATION_BATCH_SIZE)
        maxWriteBatch = maxWriteBatch.coerceAtMost(MAX_MUTATION_BATCH_SIZE)
        this.maxWriteBatchSize = maxWriteBatch

        this.mutateRpcTimeoutMs =
            mutateRpcTimeoutMs.orElse(ApiQueryConsts.DEFAULT_MUTATE_RPC_TIMEOUT_MS)
        this.readRowsRpcTimeoutMs =
            readRowsRpcTimeoutMs.orElse(ApiQueryConsts.DEFAULT_READ_ROWS_RPC_TIMEOUT_MS)
        this.shortRpcTimeoutMs =
            shortRpcTimeoutMs.orElse(ApiQueryConsts.DEFAULT_SHORT_RPC_TIMEOUT_MS)
        this.maxScanTimeoutRetries =
            maxScanTimeoutRetries.orElse(ApiQueryConsts.DEFAULT_MAX_SCAN_TIMEOUT_RETRIES)
        this.maxElapsedBackoffMs =
            maxElapsedBackoffMs.orElse(ApiQueryConsts.DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS)
    }
}