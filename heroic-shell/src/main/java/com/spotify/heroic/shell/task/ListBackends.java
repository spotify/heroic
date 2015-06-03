package com.spotify.heroic.shell.task;

import java.io.PrintWriter;
import java.util.List;

import lombok.ToString;

import org.kohsuke.args4j.Option;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.shell.CoreBridge;
import com.spotify.heroic.shell.CoreBridge.BaseParams;
import com.spotify.heroic.suggest.SuggestManager;
import com.spotify.heroic.utils.GroupMember;
import com.spotify.heroic.utils.Grouped;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

@Usage("List available backend groups")
public class ListBackends implements CoreBridge.Task {
    public static void main(String argv[]) throws Exception {
        CoreBridge.standalone(argv, ListBackends.class);
    }

    @Inject
    private MetricManager metrics;

    @Inject
    private MetadataManager metadata;

    @Inject
    private SuggestManager suggest;

    @Inject
    @Named("application/json")
    private ObjectMapper mapper;

    @Inject
    private AsyncFramework async;

    @Override
    public BaseParams params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(PrintWriter out, CoreBridge.BaseParams base) throws Exception {
        printBackends(out, "metric", metrics.getBackends());
        printBackends(out, "metadata", metadata.getBackends());
        printBackends(out, "suggest", suggest.getBackends());

        return async.resolved(null);
    }

    private void printBackends(PrintWriter out, String title, List<? extends GroupMember<? extends Grouped>> group) {
        if (group.isEmpty()) {
            out.println(String.format("%s: (empty)", title));
            return;
        }

        out.println(String.format("%s:", title));

        for (final GroupMember<? extends Grouped> grouped : group) {
            if (grouped.isDefaultMember()) {
                out.println(String.format("  %s (default) %s", grouped.getGroups(), grouped.getMember()));
                continue;
            }

            out.println(String.format("  %s %s", grouped.getGroups(), grouped.getMember()));
        }
    }

    @ToString
    private static class Parameters implements CoreBridge.BaseParams {
        @Option(name = "-c", aliases = { "--config" }, usage = "Path to configuration (only used in standalone)")
        private String config;

        @Option(name = "-h", aliases = { "--help" }, help = true, usage = "Display help")
        private boolean help;

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