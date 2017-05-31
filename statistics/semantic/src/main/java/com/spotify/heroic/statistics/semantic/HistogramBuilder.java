/*
 * Copyright (c) 2017 Spotify AB.
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

package com.spotify.heroic.statistics.semantic;

import com.codahale.metrics.Clock;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metric;
import com.spotify.metrics.core.SemanticMetricBuilder;
import java.util.concurrent.TimeUnit;

public class HistogramBuilder {
    public static final SemanticMetricBuilder<Histogram> HISTOGRAM =
        new SemanticMetricBuilder<Histogram>() {
            public Histogram newMetric() {
                return new Histogram(
                    // A min/max value will stay around for 2 * 30 seconds
                    new MinMaxSlidingTimeReservoir(Clock.defaultClock(), 2, 30, TimeUnit.SECONDS,
                        new ExponentiallyDecayingReservoir()));
            }

            public boolean isInstance(Metric metric) {
                return Histogram.class.isInstance(metric);
            }
        };
}
