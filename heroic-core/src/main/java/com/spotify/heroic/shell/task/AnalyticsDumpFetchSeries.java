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

package com.spotify.heroic.shell.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.analytics.MetricAnalytics;
import com.spotify.heroic.async.AsyncObserver;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import dagger.Component;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import lombok.Data;
import lombok.ToString;
import org.kohsuke.args4j.Option;

import javax.inject.Inject;
import javax.inject.Named;
import java.time.LocalDate;
import java.util.Optional;

@TaskUsage("Dump all fetch series values")
@TaskName("analytics-dump-fetch-series")
public class AnalyticsDumpFetchSeries implements ShellTask {
    private final MetricAnalytics metricAnalytics;
    private final ObjectMapper mapper;
    private final AsyncFramework async;

    @Inject
    public AnalyticsDumpFetchSeries(
        MetricAnalytics metricAnalytics, @Named("application/json") ObjectMapper mapper,
        AsyncFramework async
    ) {
        this.metricAnalytics = metricAnalytics;
        this.mapper = mapper;
        this.async = async;
    }

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final ResolvableFuture<Void> future = async.future();

        final LocalDate date = params.date.map(LocalDate::parse).orElseGet(LocalDate::now);

        metricAnalytics.seriesHits(date).observe(AsyncObserver.bind(future, series -> {
            try {
                io
                    .out()
                    .println(mapper.writeValueAsString(new AnalyticsHits(series.getSeries(),
                        series.getSeries().getHashCode().toString(), series.getHits())));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

            //io.out().flush();
            return async.resolved();
        }));

        return future;
    }

    @ToString
    private static class Parameters extends AbstractShellTaskParams {
        @Option(name = "-d", aliases = {"--date"}, usage = "Date to fetch data for",
            metaVar = "<yyyy-MM-dd>")
        private Optional<String> date = Optional.empty();
    }

    @Data
    public static class AnalyticsHits {
        private final Series series;
        private final String id;
        private final long hits;
    }

    public static AnalyticsDumpFetchSeries setup(final CoreComponent core) {
        return DaggerAnalyticsDumpFetchSeries_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    interface C {
        AnalyticsDumpFetchSeries task();
    }
}
