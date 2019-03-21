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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.analytics.MetricAnalytics;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.shell.Tasks;
import dagger.Component;
import eu.toolchain.async.AsyncFuture;
import java.time.LocalDate;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Named;
import org.kohsuke.args4j.Option;

@TaskUsage("Report that a series has been fetched")
@TaskName("analytics-report-fetch-series")
public class AnalyticsReportFetchSeries implements ShellTask {
    private final MetricAnalytics metricAnalytics;
    private final ObjectMapper mapper;

    @Inject
    public AnalyticsReportFetchSeries(
        MetricAnalytics metricAnalytics, @Named("application/json") ObjectMapper mapper
    ) {
        this.metricAnalytics = metricAnalytics;
        this.mapper = mapper;
    }

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final Series series = Tasks.parseSeries(mapper, params.series);
        final LocalDate date = params.date.map(LocalDate::parse).orElseGet(LocalDate::now);
        return metricAnalytics.reportFetchSeries(date, series);
    }

    private static class Parameters extends AbstractShellTaskParams {
        @Option(name = "-s", aliases = {"--series"}, usage = "Series to report fetch for",
            metaVar = "<json>")
        private Optional<String> series = Optional.empty();

        @Option(name = "-d", aliases = {"--date"}, usage = "Date to fetch data for",
            metaVar = "<yyyy-MM-dd>")
        private Optional<String> date = Optional.empty();

        public String toString() {
            return "AnalyticsReportFetchSeries.Parameters(series=" + this.series + ", date="
                   + this.date
                   + ")";
        }
    }

    public static AnalyticsReportFetchSeries setup(final CoreComponent core) {
        return DaggerAnalyticsReportFetchSeries_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    interface C {
        AnalyticsReportFetchSeries task();
    }
}
