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

package com.spotify.heroic.metric.bigtable.api;

import com.spotify.heroic.bigtable.com.google.protobuf.ByteString;;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.Optional;

@Data
public class ReadRowsRequest {
    private final Optional<RowRange> range;
    private final Optional<RowFilter> filter;
    private final Optional<ByteString> rowKey;

    public com.google.bigtable.v2.ReadRowsRequest toPb(final String tableUri) {
        final com.google.bigtable.v2.RowSet.Builder rowSetBuilder =
          com.google.bigtable.v2.RowSet.newBuilder();

        range.map(RowRange::toPb).ifPresent(rowSetBuilder::addRowRanges);
        rowKey.ifPresent(rowSetBuilder::addRowKeys);

        final com.google.bigtable.v2.ReadRowsRequest.Builder requestBuilder =
            com.google.bigtable.v2.ReadRowsRequest.newBuilder();

        requestBuilder.setTableName(tableUri);
        requestBuilder.setRows(rowSetBuilder.build());

        filter.map(RowFilter::toPb).ifPresent(requestBuilder::setFilter);

        return requestBuilder.build();
    }

    public static Builder builder() {
        return new Builder();
    }

    @RequiredArgsConstructor
    public static class Builder {
        private Optional<RowRange> range = Optional.empty();
        private Optional<RowFilter> filter = Optional.empty();
        private Optional<ByteString> rowKey = Optional.empty();

        public Builder range(final RowRange range) {
            this.range = Optional.of(range);
            return this;
        }

        public Builder filter(final RowFilter filter) {
            this.filter = Optional.of(filter);
            return this;
        }

        public Builder rowKey(final ByteString rowKey) {
            this.rowKey = Optional.of(rowKey);
            return this;
        }

        public ReadRowsRequest build() {
            return new ReadRowsRequest(range, filter, rowKey);
        }
    }
}
