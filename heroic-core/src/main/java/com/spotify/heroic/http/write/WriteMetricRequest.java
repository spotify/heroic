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

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.ingestion.Ingestion;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricCollection;
import lombok.Data;

import java.util.Optional;

@Data
public class WriteMetricRequest {
    final Optional<Series> series;
    final Optional<MetricCollection> data;

    public boolean isEmpty() {
        return data.map(MetricCollection::isEmpty).orElse(true);
    }

    public Iterable<Metric> all() {
        return data.map(d -> d.getDataAs(Metric.class)).orElseGet(ImmutableList::of);
    }

    public Ingestion.Request toIngestionRequest() {
        return new Ingestion.Request(series.orElseGet(Series::empty),
            data.orElseGet(MetricCollection::empty));
    }
}
