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

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.ToString;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import com.google.inject.Inject;
import com.spotify.heroic.HeroicShell;
import com.spotify.heroic.common.RangeFilter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.metadata.CountSeries;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataEntry;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.shell.AbstractShellTask;
import com.spotify.heroic.shell.ShellTaskParams;
import com.spotify.heroic.shell.ShellTaskUsage;
import com.spotify.heroic.shell.Tasks;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Transform;

@ShellTaskUsage("Fetch series matching the given query")
public class MetadataMigrate extends AbstractShellTask {
    public static final int DOT_LIMIT = 10000;
    public static final int LINE_LIMIT = 20;

    public static void main(String argv[]) throws Exception {
        HeroicShell.standalone(argv, MetadataMigrate.class);
    }

    @Inject
    private MetadataManager metadata;

    @Inject
    private QueryParser parser;

    @Inject
    private FilterFactory filters;

    @Override
    public ShellTaskParams params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final PrintWriter out, ShellTaskParams base) throws Exception {
        final Parameters params = (Parameters) base;

        final RangeFilter filter = Tasks.setupRangeFilter(filters, parser, params);

        final MetadataBackend group = metadata.useGroup(params.group);
        final MetadataBackend target = metadata.useGroup(params.target);

        out.println("Migrating:");
        out.println("  from: " + group);
        out.println("    to: " + target);

        return group.countSeries(filter).transform(new Transform<CountSeries, Void>() {
            @Override
            public Void transform(CountSeries c) throws Exception {
                final long count = c.getCount();

                out.println(String.format("Migrating %d entrie(s)", count));

                if (!params.ok) {
                    out.println("Migration stopped, use --ok to proceed");
                    return null;
                }

                final Iterable<MetadataEntry> entries = group.entries(filter.getFilter(), filter.getRange());

                int index = 1;

                for (final MetadataEntry e : entries) {
                    if (index % DOT_LIMIT == 0) {
                        out.print(".");
                        out.flush();
                    }

                    if (index % (DOT_LIMIT * LINE_LIMIT) == 0) {
                        out.println(String.format(" %d/%d", index, count));
                        out.flush();
                    }

                    ++index;
                    target.write(e.getSeries(), filter.getRange());
                }

                out.println(String.format(" %d/%d", index, count));
                return null;
            }
        });
    }

    @ToString
    private static class Parameters extends Tasks.QueryParamsBase {
        @Option(name = "-g", aliases = { "--group" }, usage = "Backend group to migrate from", metaVar = "<metadata-group>", required = true)
        private String group;

        @Option(name = "-t", aliases = { "--target" }, usage = "Backend group to migrate to", metaVar = "<metadata-group>", required = true)
        private String target;

        @Option(name = "--ok", usage = "Verify the migration")
        private boolean ok = false;

        @Option(name = "--limit", aliases = { "--limit" }, usage = "Limit the number of printed entries")
        @Getter
        private int limit = 10;

        @Argument
        @Getter
        private List<String> query = new ArrayList<String>();
    }
}