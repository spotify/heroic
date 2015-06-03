package com.spotify.heroic.shell.task;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.ToString;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.model.RangeFilter;
import com.spotify.heroic.shell.CoreBridge;
import com.spotify.heroic.shell.CoreBridge.BaseParams;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Transform;

@Usage("Get tags")
public class MetadataTags implements CoreBridge.Task {
    public static void main(String argv[]) throws Exception {
        CoreBridge.standalone(argv, MetadataTags.class);
    }

    @Inject
    private MetadataManager metadata;

    @Inject
    private FilterFactory filters;

    @Inject
    private QueryParser parser;

    @Inject
    @Named("application/json")
    private ObjectMapper mapper;

    @Override
    public BaseParams params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final PrintWriter out, BaseParams base) throws Exception {
        final Parameters params = (Parameters) base;

        final RangeFilter filter = Tasks.setupRangeFilter(filters, parser, params);

        return metadata.useGroup(params.group).findTags(filter).transform(new Transform<FindTags, Void>() {
            @Override
            public Void transform(FindTags result) throws Exception {
                out.println(result.toString());
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
