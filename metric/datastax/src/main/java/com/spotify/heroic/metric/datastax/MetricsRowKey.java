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

package com.spotify.heroic.metric.datastax;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.common.Series;
import eu.toolchain.serializer.AutoSerialize;

// NB(hexedpackets): This should be a kotlin data class, but the AutoSerialize annotation does not
// compile on kotlin classes.
@AutoSerialize
public class MetricsRowKey {
    private final Series series;
    @AutoSerialize.Field(provided = true)
    private final long base;

    public MetricsRowKey(Series series, long base) {
        this.series = series;
        this.base = base;
    }

    @JsonCreator
    public static MetricsRowKey create(
        @JsonProperty("series") Series series, @JsonProperty("base") Long base
    ) {
        return new MetricsRowKey(series, base);
    }

    public Series getSeries() {
        return this.series;
    }

    public long getBase() {
        return this.base;
    }
}
