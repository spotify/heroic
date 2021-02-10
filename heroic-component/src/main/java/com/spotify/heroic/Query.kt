/*
 * Copyright (c) 2019 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"): you may not use this file except in compliance
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

package com.spotify.heroic

import com.spotify.heroic.aggregation.Aggregation
import com.spotify.heroic.common.FeatureSet
import com.spotify.heroic.filter.Filter
import com.spotify.heroic.metric.MetricType
import java.util.*

data class Query(
    val aggregation: Optional<Aggregation>,
    val source: Optional<MetricType>, // points or distribution points for testing
    val range: Optional<QueryDateRange>,
    val filter: Optional<Filter>,
    val options: Optional<QueryOptions>,
    val features: Optional<FeatureSet> // set of experimental features to enable
)
