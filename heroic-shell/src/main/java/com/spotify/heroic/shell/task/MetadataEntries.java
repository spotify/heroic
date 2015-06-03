package com.spotify.heroic.shell.task;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.ToString;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import com.google.inject.Inject;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.metadata.model.CountSeries;
import com.spotify.heroic.metadata.model.MetadataEntry;
import com.spotify.heroic.model.RangeFilter;
import com.spotify.heroic.shell.CoreBridge;
import com.spotify.heroic.shell.CoreBridge.BaseParams;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Transform;

@Usage("Fetch series matching the given query")
public class MetadataEntries implements CoreBridge.Task {
    public static void main(String argv[]) throws Exception {
        CoreBridge.standalone(argv, MetadataEntries.class);
    }

    @Inject
    private MetadataManager metadata;

    @Inject
    private QueryParser parser;

    @Inject
    private FilterFactory filters;

    @Override
    public BaseParams params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final PrintWriter out, BaseParams base) throws Exception {
        final Parameters params = (Parameters) base;

        final RangeFilter filter = Tasks.setupRangeFilter(filters, parser, params);

        final MetadataBackend group = metadata.useGroup(params.group);

        return group.countSeries(filter).transform(new Transform<CountSeries, Void>() {
            @Override
            public Void transform(CountSeries c) throws Exception {
                Iterable<MetadataEntry> entries = group.entries(filter.getFilter(), filter.getRange());

                int i = 1;
                final long count = c.getCount();

                for (final MetadataEntry e : entries) {
                    out.println(String.format("%d/%d: %s", i++, count, e));
                    out.flush();
                }

                out.println(String.format("%d entrie(s)", (i - 1)));
                return null;
            }
        });
    }

    @ToString
    private static class Parameters extends Tasks.QueryParamsBase implements CoreBridge.BaseParams, Tasks.QueryParams {
        @Option(name = "-c", aliases = { "--config" }, usage = "Path to configuration (only used in standalone)", metaVar = "<config>")
        private String config;

        @Option(name = "-g", aliases = { "--group" }, usage = "Backend group to use", metaVar = "<group>")
        private String group;

        @Option(name = "-h", aliases = { "--help" }, help = true, usage = "Display help")
        private boolean help;

        @Option(name = "--limit", aliases = { "--limit" }, usage = "Limit the number of printed entries")
        @Getter
        private int limit = 10;

        @Argument
        @Getter
        private List<String> query = new ArrayList<String>();

        @Override
        public String config() {
            return config;
        }

        @Override
        public boolean help() {
            return help;
        }
    }
}