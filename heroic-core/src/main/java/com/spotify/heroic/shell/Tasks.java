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

package com.spotify.heroic.shell;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.HeroicCoreInstance;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.RangeFilter;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.metric.BackendKeyFilter;
import com.spotify.heroic.shell.task.AnalyticsDumpFetchSeries;
import com.spotify.heroic.shell.task.AnalyticsReportFetchSeries;
import com.spotify.heroic.shell.task.BackendKeyArgument;
import com.spotify.heroic.shell.task.ConfigGet;
import com.spotify.heroic.shell.task.Configure;
import com.spotify.heroic.shell.task.CountData;
import com.spotify.heroic.shell.task.DataMigrate;
import com.spotify.heroic.shell.task.DeleteKeys;
import com.spotify.heroic.shell.task.DeserializeKey;
import com.spotify.heroic.shell.task.Fetch;
import com.spotify.heroic.shell.task.IngestionFilter;
import com.spotify.heroic.shell.task.Keys;
import com.spotify.heroic.shell.task.ListBackends;
import com.spotify.heroic.shell.task.MetadataCount;
import com.spotify.heroic.shell.task.MetadataDelete;
import com.spotify.heroic.shell.task.MetadataEntries;
import com.spotify.heroic.shell.task.MetadataFetch;
import com.spotify.heroic.shell.task.MetadataLoad;
import com.spotify.heroic.shell.task.MetadataMigrate;
import com.spotify.heroic.shell.task.MetadataTags;
import com.spotify.heroic.shell.task.ParseQuery;
import com.spotify.heroic.shell.task.Pause;
import com.spotify.heroic.shell.task.Query;
import com.spotify.heroic.shell.task.ReadWriteTest;
import com.spotify.heroic.shell.task.Resume;
import com.spotify.heroic.shell.task.SerializeKey;
import com.spotify.heroic.shell.task.Statistics;
import com.spotify.heroic.shell.task.StringifyQuery;
import com.spotify.heroic.shell.task.SuggestKey;
import com.spotify.heroic.shell.task.SuggestPerformance;
import com.spotify.heroic.shell.task.SuggestTag;
import com.spotify.heroic.shell.task.SuggestTagKeyCount;
import com.spotify.heroic.shell.task.SuggestTagValue;
import com.spotify.heroic.shell.task.SuggestTagValues;
import com.spotify.heroic.shell.task.Write;
import com.spotify.heroic.shell.task.WritePerformance;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.DateTimeParserBucket;
import org.kohsuke.args4j.Option;

import lombok.Getter;

public final class Tasks {
    static final List<ShellTaskDefinition> available = new ArrayList<>();

    static {
        available.add(shellTask(Configure.class));
        available.add(shellTask(Statistics.class));
        available.add(shellTask(ConfigGet.class));
        available.add(shellTask(Keys.class));
        available.add(shellTask(DeleteKeys.class));
        available.add(shellTask(CountData.class));
        available.add(shellTask(SerializeKey.class));
        available.add(shellTask(DeserializeKey.class));
        available.add(shellTask(ListBackends.class));
        available.add(shellTask(Fetch.class));
        available.add(shellTask(Write.class));
        available.add(shellTask(WritePerformance.class));
        available.add(shellTask(MetadataDelete.class));
        available.add(shellTask(MetadataFetch.class));
        available.add(shellTask(MetadataTags.class));
        available.add(shellTask(MetadataCount.class));
        available.add(shellTask(MetadataEntries.class));
        available.add(shellTask(MetadataMigrate.class));
        available.add(shellTask(MetadataLoad.class));
        available.add(shellTask(SuggestTag.class));
        available.add(shellTask(SuggestKey.class));
        available.add(shellTask(SuggestTagValue.class));
        available.add(shellTask(SuggestTagValues.class));
        available.add(shellTask(SuggestTagKeyCount.class));
        available.add(shellTask(SuggestPerformance.class));
        available.add(shellTask(Query.class));
        available.add(shellTask(ReadWriteTest.class));
        available.add(shellTask(Pause.class));
        available.add(shellTask(Resume.class));
        available.add(shellTask(IngestionFilter.class));
        available.add(shellTask(DataMigrate.class));
        available.add(shellTask(ParseQuery.class));
        available.add(shellTask(StringifyQuery.class));
        available.add(shellTask(AnalyticsReportFetchSeries.class));
        available.add(shellTask(AnalyticsDumpFetchSeries.class));
    }

    public static List<ShellTaskDefinition> available() {
        return available;
    }

    static ShellTaskDefinition shellTask(final Class<? extends ShellTask> task) {
        final String usage = taskUsage(task);

        final String name = name(task);
        final List<String> names = allNames(task);
        final List<String> aliases = aliases(task);

        return new ShellTaskDefinition() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public List<String> names() {
                return names;
            }

            @Override
            public List<String> aliases() {
                return aliases;
            }

            @Override
            public String usage() {
                return usage;
            }

            @Override
            public ShellTask setup(final HeroicCoreInstance core) throws Exception {
                return core.inject(newInstance(task));
            };
        };
    }

    public static String taskUsage(final Class<? extends ShellTask> task) {
        final TaskUsage u = task.getAnnotation(TaskUsage.class);

        if (u != null) {
            return u.value();
        }

        return String.format("<no @ShellTaskUsage annotation for %s>", task.getCanonicalName());
    }

    public static String name(final Class<? extends ShellTask> task) {
        final TaskName n = task.getAnnotation(TaskName.class);

        if (n != null) {
            return n.value();
        }

        throw new IllegalStateException(
                String.format("No name configured with @TaskName on %s", task.getCanonicalName()));
    }

    public static List<String> allNames(final Class<? extends ShellTask> task) {
        final TaskName n = task.getAnnotation(TaskName.class);
        final List<String> names = new ArrayList<>();

        if (n != null) {
            names.add(n.value());

            for (final String alias : n.aliases()) {
                names.add(alias);
            }
        }

        if (names.isEmpty()) {
            throw new IllegalStateException(String.format("No name configured with @TaskName on %s",
                    task.getCanonicalName()));
        }

        return names;
    }

    public static List<String> aliases(final Class<? extends ShellTask> task) {
        final TaskName n = task.getAnnotation(TaskName.class);
        final List<String> names = new ArrayList<>();

        if (n != null) {
            for (final String alias : n.aliases()) {
                names.add(alias);
            }
        }

        return names;
    }

    public static ShellTask newInstance(Class<? extends ShellTask> taskType) throws Exception {
        final Constructor<? extends ShellTask> constructor;

        try {
            constructor = taskType.getConstructor();
        } catch (ReflectiveOperationException e) {
            throw new Exception("Task '" + taskType.getCanonicalName()
                    + "' does not have an accessible, empty constructor", e);
        }

        try {
            return constructor.newInstance();
        } catch (ReflectiveOperationException e) {
            throw new Exception("Failed to invoke constructor of '" + taskType.getCanonicalName(),
                    e);
        }
    }

    public static Filter setupFilter(FilterFactory filters, QueryParser parser,
            TaskQueryParameters params) {
        final List<String> query = params.getQuery();

        if (query.isEmpty()) {
            return filters.t();
        }

        return parser.parseFilter(StringUtils.join(query, " "));
    }

    public static BackendKeyFilter setupKeyFilter(KeyspaceBase params, ObjectMapper mapper)
            throws Exception {
        BackendKeyFilter filter = BackendKeyFilter.of();

        if (params.start != null) {
            filter = filter.withStart(BackendKeyFilter
                    .gte(mapper.readValue(params.start, BackendKeyArgument.class).toBackendKey()));
        }

        if (params.startPercentage >= 0) {
            filter = filter.withStart(
                    BackendKeyFilter.gtePercentage((float) params.startPercentage / 100f));
        }

        if (params.startToken != null) {
            filter = filter.withStart(BackendKeyFilter.gteToken(params.startToken));
        }

        if (params.end != null) {
            filter = filter.withEnd(BackendKeyFilter
                    .lt(mapper.readValue(params.end, BackendKeyArgument.class).toBackendKey()));
        }

        if (params.endPercentage >= 0) {
            filter = filter
                    .withEnd(BackendKeyFilter.ltPercentage((float) params.endPercentage / 100f));
        }

        if (params.endToken != null) {
            filter = filter.withEnd(BackendKeyFilter.ltToken(params.endToken));
        }

        if (params.limit >= 0) {
            filter = filter.withLimit(params.limit);
        }

        return filter;
    }

    public abstract static class QueryParamsBase extends AbstractShellTaskParams
            implements TaskQueryParameters {
        private final DateRange defaultDateRange;

        public QueryParamsBase() {
            final long now = System.currentTimeMillis();
            final long start = now - TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS);
            this.defaultDateRange = new DateRange(start, now);
        }

        @Override
        public DateRange getRange() {
            return defaultDateRange;
        }
    }

    public abstract static class KeyspaceBase extends QueryParamsBase {
        @Option(name = "--start", usage = "First key to operate on", metaVar = "<json>")
        protected String start;

        @Option(name = "--end", usage = "Last key to operate on (exclusive)", metaVar = "<json>")
        protected String end;

        @Option(name = "--start-percentage", usage = "First key to operate on in percentage",
                metaVar = "<int>")
        protected int startPercentage = -1;

        @Option(name = "--end-percentage",
                usage = "Last key to operate on (exclusive) in percentage", metaVar = "<int>")
        protected int endPercentage = -1;

        @Option(name = "--start-token", usage = "First token to operate on", metaVar = "<long>")
        protected Long startToken = null;

        @Option(name = "--end-token", usage = "Last token to operate on (exclusive)",
                metaVar = "<int>")
        protected Long endToken = null;

        @Option(name = "--limit", usage = "Limit the number keys to operate on", metaVar = "<int>")
        @Getter
        protected int limit = -1;
    }

    public static RangeFilter setupRangeFilter(FilterFactory filters, QueryParser parser,
            TaskQueryParameters params) {
        final Filter filter = setupFilter(filters, parser, params);
        return new RangeFilter(filter, params.getRange(), params.getLimit());
    }

    private static final List<DateTimeParser> today = new ArrayList<>();
    private static final List<DateTimeParser> full = new ArrayList<>();

    static {
        today.add(DateTimeFormat.forPattern("HH:mm").getParser());
        today.add(DateTimeFormat.forPattern("HH:mm:ss").getParser());
        today.add(DateTimeFormat.forPattern("HH:mm:ss.SSS").getParser());
        full.add(DateTimeFormat.forPattern("yyyy-MM-dd/HH:mm").getParser());
        full.add(DateTimeFormat.forPattern("yyyy-MM-dd/HH:mm:ss").getParser());
        full.add(DateTimeFormat.forPattern("yyyy-MM-dd/HH:mm:ss.SSS").getParser());
    }

    public static long parseInstant(String input, long now) {
        if (input.charAt(0) == '+') {
            return now + Long.parseLong(input.substring(1));
        }

        if (input.charAt(0) == '-') {
            return now - Long.parseLong(input.substring(1));
        }

        // try to parse just milliseconds
        try {
            return Long.parseLong(input);
        } catch (IllegalArgumentException e) {
            // pass-through
        }

        final Chronology chrono = ISOChronology.getInstanceUTC();

        if (input.indexOf('/') >= 0) {
            return parseFullInstant(input, chrono);
        }

        return parseTodayInstant(input, chrono, now);
    }

    private static long parseTodayInstant(String input, final Chronology chrono, long now) {
        final DateTime n = new DateTime(now, chrono);

        for (final DateTimeParser p : today) {
            final DateTimeParserBucket bucket =
                    new DateTimeParserBucket(0, chrono, null, null, 2000);

            bucket.saveField(chrono.year(), n.getYear());
            bucket.saveField(chrono.monthOfYear(), n.getMonthOfYear());
            bucket.saveField(chrono.dayOfYear(), n.getDayOfYear());

            try {
                p.parseInto(bucket, input, 0);
            } catch (IllegalArgumentException e) {
                // pass-through
                continue;
            }

            return bucket.computeMillis();
        }

        throw new IllegalArgumentException(input + " is not a valid instant");
    }

    private static long parseFullInstant(String input, final Chronology chrono) {
        for (final DateTimeParser p : full) {
            final DateTimeParserBucket bucket =
                    new DateTimeParserBucket(0, chrono, null, null, 2000);

            try {
                p.parseInto(bucket, input, 0);
            } catch (IllegalArgumentException e) {
                // pass-through
                continue;
            }

            return bucket.computeMillis();
        }

        throw new IllegalArgumentException(input + " is not a valid instant");
    }

    public static String formatTimeNanos(long diff) {
        if (diff < 1000) {
            return String.format("%d ns", diff);
        }

        if (diff < 1000000) {
            final double v = ((double) diff) / 1000;
            return String.format("%.3f us", v);
        }

        if (diff < 1000000000) {
            final double v = ((double) diff) / 1000000;
            return String.format("%.3f ms", v);
        }

        final double v = ((double) diff) / 1000000000;
        return String.format("%.3f s", v);
    }
}
