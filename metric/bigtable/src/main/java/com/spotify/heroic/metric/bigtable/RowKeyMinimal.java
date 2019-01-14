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

import eu.toolchain.serializer.AutoSerialize;
import java.util.SortedMap;
import lombok.Data;

@AutoSerialize
@Data
public class RowKeyMinimal {
    private final Series series;
    private final long base;

    @AutoSerialize
    @Data
    static class Series {
        private final String key;
        private final SortedMap<String, String> tags;

        public static Series create(com.spotify.heroic.common.Series series) {
            return new Series(series.getKey(), series.getTags());
        }
    }

    public static RowKeyMinimal create(RowKey rowKey) {
        return new RowKeyMinimal(Series.create(rowKey.getSeries()), rowKey.getBase());
    }
}
