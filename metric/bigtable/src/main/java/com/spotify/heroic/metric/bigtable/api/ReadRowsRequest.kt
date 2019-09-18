/*
 * Copyright (c) 2019 Spotify AB.
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

package com.spotify.heroic.metric.bigtable.api

import com.google.protobuf.ByteString
import java.util.*

data class ReadRowsRequest(
    val range: Optional<RowRange>,
    val filter: Optional<RowFilter>,
    val rowKey: Optional<ByteString>
) {
    fun toPb(tableUri: String): com.google.bigtable.v2.ReadRowsRequest {
        val rowSetBuilder = com.google.bigtable.v2.RowSet.newBuilder()
        range.map(RowRange::toPb).ifPresent { rowSetBuilder.addRowRanges(it) }
        rowKey.ifPresent { rowSetBuilder.addRowKeys(it) }

        val requestBuilder = com.google.bigtable.v2.ReadRowsRequest.newBuilder()
            .setTableName(tableUri)
            .setRows(rowSetBuilder.build())
        filter.map(RowFilter::toPb).ifPresent { requestBuilder.filter = it }
        return requestBuilder.build()
    }

    companion object {
        class Builder(
            var range: RowRange? = null,
            var filter: RowFilter? = null,
            var rowKey: ByteString? = null
        ) {
            fun range(range: RowRange): Builder {
                this.range = range
                return this
            }

            fun filter(filter: RowFilter): Builder {
                this.filter = filter
                return this
            }

            fun rowKey(rowKey: ByteString): Builder {
                this.rowKey = rowKey
                return this
            }

            fun build(): ReadRowsRequest = ReadRowsRequest(
                Optional.ofNullable(range),
                Optional.ofNullable(filter),
                Optional.ofNullable(rowKey))
        }

        @JvmStatic fun builder(): Builder = Builder()
    }
}