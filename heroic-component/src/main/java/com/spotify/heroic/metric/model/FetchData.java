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

package com.spotify.heroic.metric.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import lombok.Data;

import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.TimeData;

import eu.toolchain.async.Collector;

@Data
public class FetchData<T extends TimeData> {
    private final Series series;
    private final List<T> data;
    private final List<Long> times;

    public static <T extends TimeData> Collector<FetchData<T>, FetchData<T>> merger(final Series series) {
        return new Collector<FetchData<T>, FetchData<T>>() {
            @Override
            public FetchData<T> collect(Collection<FetchData<T>> results) throws Exception {
                final List<T> data = new ArrayList<>();
                final List<Long> times = new ArrayList<Long>();

                for (final FetchData<T> fetch : results) {
                    data.addAll(fetch.getData());
                    times.addAll(fetch.getTimes());
                }

                Collections.sort(data);
                return new FetchData<T>(series, data, times);
            }
        };
    }
}