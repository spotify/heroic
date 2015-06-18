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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.Aggregation.Group;
import com.spotify.heroic.metric.model.FetchData;
import com.spotify.heroic.metric.model.ResultGroup;
import com.spotify.heroic.metric.model.ResultGroups;
import com.spotify.heroic.metric.model.TagValues;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Statistics;
import com.spotify.heroic.model.TimeData;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.StreamCollector;

@Slf4j
@RequiredArgsConstructor
public class AggregatedCallbackStream<T extends TimeData> implements StreamCollector<FetchData<T>, ResultGroups> {
    private final List<TagValues> group;
    private final Aggregation.Session session;
    private final List<? extends AsyncFuture<?>> fetches;

    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    @Override
    public void resolved(final FetchData<T> result) throws Exception {
        session.update(new Aggregation.Group(result.getSeries().getTags(), result.getData()));
    }

    @Override
    public void failed(final Throwable error) throws Exception {
        log.error("Request failed: " + error.toString(), error);
        cancelRest();
    }

    @Override
    public void cancelled() throws Exception {
        cancelRest();
    }

    private void cancelRest() {
        if (!cancelled.compareAndSet(false, true))
            return;

        for (final AsyncFuture<?> future : fetches)
            future.cancel();
    }

    @SuppressWarnings("unchecked")
    @Override
    public ResultGroups end(final int successful, final int failed, final int cancelled) throws Exception {
        if (failed > 0 || cancelled > 0)
            throw new Exception("Some series were not fetched from the database");

        final Aggregation.Result result = session.result();

        if (result.getResult().size() != 1)
            throw new Exception("only expected one result group");

        final Group g = result.getResult().iterator().next();

        final Statistics stat = Statistics.builder().aggregator(result.getStatistics())
                .row(new Statistics.Row(successful, failed)).build();

        final Class<?> output = session.output();
        final List<ResultGroup> groups = new ArrayList<>();
        if (DataPoint.class.isAssignableFrom(output)) {
            groups.add(new ResultGroup.DataPointResultGroup(this.group, (List<DataPoint>) g.getValues()));
        }
        return ResultGroups.fromResult(groups, stat);
    }
}