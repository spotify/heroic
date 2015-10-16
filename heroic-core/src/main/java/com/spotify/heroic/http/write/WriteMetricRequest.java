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

package com.spotify.heroic.http.write;

import static java.util.Optional.ofNullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.WriteMetric;

import lombok.Data;

@Data
public class WriteMetricRequest {
    final Series series;
    final MetricCollection data;

    @JsonCreator
    public WriteMetricRequest(@JsonProperty("series") Series series, @JsonProperty("data") MetricCollection data) {
        this.series = ofNullable(series).orElseGet(Series::empty);
        this.data = ofNullable(data).orElseGet(MetricCollection::empty);
    }

    public boolean isEmpty() {
        return data.isEmpty();
    }

    public Iterable<Metric> all() {
        return data.getDataAs(Metric.class);
    }

    public WriteMetric toWriteMetric() {
        return new WriteMetric(series, data);
    }
}