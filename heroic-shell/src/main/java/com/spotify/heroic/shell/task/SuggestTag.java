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
import com.spotify.heroic.model.RangeFilter;
import com.spotify.heroic.shell.CoreBridge;
import com.spotify.heroic.shell.CoreBridge.BaseParams;
import com.spotify.heroic.suggest.SuggestManager;
import com.spotify.heroic.suggest.model.MatchOptions;
import com.spotify.heroic.suggest.model.TagSuggest;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Transform;

@Usage("Fetch series matching the given query")
public class SuggestTag implements CoreBridge.Task {
    public static void main(String argv[]) throws Exception {
        CoreBridge.standalone(argv, SuggestTag.class);
    }

    @Inject
    private SuggestManager suggest;

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

        final MatchOptions fuzzyOptions = MatchOptions.builder().build();

        return suggest.useGroup(params.group).tagSuggest(filter, fuzzyOptions, params.key, params.value)
                .transform(new Transform<TagSuggest, Void>() {
                    @Override
                    public Void transform(TagSuggest result) throws Exception {
                        int i = 0;

                        for (final TagSuggest.Suggestion suggestion : result.getSuggestions()) {
                            out.println(String.format("%s: %s", i++, suggestion));
                        }

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

        @Option(name = "-k", aliases = { "--key" }, usage = "Provide key context for suggestion")
        private String key = null;

        @Option(name = "-v", aliases = { "--value" }, usage = "Provide value context for suggestion")
        private String value = null;

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