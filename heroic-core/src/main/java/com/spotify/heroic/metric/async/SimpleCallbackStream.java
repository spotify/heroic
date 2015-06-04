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

package com.spotify.heroic.metric.async;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.metric.model.FetchData;
import com.spotify.heroic.metric.model.ResultGroup;
import com.spotify.heroic.metric.model.ResultGroups;
import com.spotify.heroic.metric.model.TagValues;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.Statistics;
import com.spotify.heroic.model.TimeData;

import eu.toolchain.async.StreamCollector;

@Slf4j
@RequiredArgsConstructor
public final class SimpleCallbackStream<T extends TimeData> implements StreamCollector<FetchData<T>, ResultGroups> {
    private final Queue<FetchData<T>> results = new ConcurrentLinkedQueue<FetchData<T>>();

    private final Class<T> type;

    @Override
    public void resolved(FetchData<T> result) throws Exception {
        results.add(result);
    }

    @Override
    public void failed(Throwable error) throws Exception {
        log.error("Error encountered when processing request", error);
    }

    @Override
    public void cancelled() throws Exception {
        log.error("Request cancelled");
    }

    @Override
    public ResultGroups end(int successful, int failed, int cancelled) throws Exception {
        if (failed > 0)
            throw new Exception("Some time series could not be fetched from the database");

        final Map<Series, List<T>> results = new HashMap<>();

        int sampleSize = 0;

        for (final FetchData<T> result : this.results) {
            List<T> data = results.get(result.getSeries().getTags());

            if (data == null) {
                data = new ArrayList<T>();
                results.put(result.getSeries(), data);
            }

            data.addAll(result.getData());
            sampleSize += result.getData().size();
        }

        final Statistics stat = Statistics.builder().row(new Statistics.Row(successful, failed))
                .aggregator(new Statistics.Aggregator(sampleSize, 0, 0)).build();

        final List<ResultGroup> groups = new ArrayList<>();

        for (final Map.Entry<Series, List<T>> e : results.entrySet()) {
            Collections.sort(e.getValue());
            groups.add(new ResultGroup(tagsFor(e.getKey()), e.getValue(), type));
        }

        return ResultGroups.fromResult(groups, stat);
    }

    private List<TagValues> tagsFor(Series key) {
        final List<TagValues> tags = new ArrayList<>();

        for (final Map.Entry<String, String> e : key.getTags().entrySet())
            tags.add(new TagValues(e.getKey(), ImmutableList.of(e.getValue())));

        return tags;
    }
}