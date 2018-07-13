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

import java.util.Optional;

@Data
public class RowRange {
    private final Optional<ByteString> start;
    private final Optional<ByteString> end;

    /**
     * Build a new row range.
     *
     * @param start Start key to start at (inclusive).
     * @param end End key to stop at (exclusive).
     * @return A row range with the given parameters.
     */
    public static RowRange rowRange(Optional<ByteString> start, Optional<ByteString> end) {
        return new RowRange(start, end);
    }

    public com.google.bigtable.v2.RowRange toPb() {
        final com.google.bigtable.v2.RowRange.Builder builder =
            com.google.bigtable.v2.RowRange.newBuilder();
        start.ifPresent(builder::setStartKeyClosed);
        end.ifPresent(builder::setEndKeyOpen);
        return builder.build();
    }
}
