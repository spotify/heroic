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
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.analytics.MetricAnalytics;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;

import java.time.LocalDate;
import java.util.Optional;

import org.kohsuke.args4j.Option;

import eu.toolchain.async.AsyncFuture;
import lombok.ToString;

@TaskUsage("Report that a series has been fetched")
@TaskName("analytics-report-fetch-series")
public class AnalyticsReportFetchSeries implements ShellTask {
    @Inject
    private MetricAnalytics metricAnalytics;

    @Inject
    @Named("application/json")
    private ObjectMapper mapper;

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final Series series;

        if (params.series == null) {
            series = Series.empty();
        } else {
            series = mapper.readValue(params.series, Series.class);
        }

        final LocalDate date =
                Optional.ofNullable(params.date).map(LocalDate::parse).orElseGet(LocalDate::now);

        return metricAnalytics.reportFetchSeries(date, series);
    }

    @ToString
    private static class Parameters extends AbstractShellTaskParams {
        @Option(name = "-s", aliases = {"--series"}, usage = "Series to report fetch for",
                metaVar = "<json>")
        private String series;

        @Option(name = "-d", aliases = {"--date"}, usage = "Date to fetch data for",
                metaVar = "<yyyy-MM-dd>")
        private String date = null;
    }
}
