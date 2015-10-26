package com.spotify.heroic.shell.task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.MetricBackends;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.QueryOptions;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.shell.Tasks;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureDone;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@TaskUsage("Migrate data from one backend to another")
@TaskName("data-migrate")
@Slf4j
public class DataMigrate implements ShellTask {
    @Inject
    private FilterFactory filters;

    @Inject
    private QueryParser parser;

    @Inject
    private MetricManager metric;

    @Inject
    @Named("application/json")
    private ObjectMapper mapper;

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, final TaskParameters p) throws Exception {
        final Parameters params = (Parameters)p;

        final BackendKey start;

        if (params.start != null) {
            start = mapper.readValue(params.start, BackendKeyArgument.class).toBackendKey();
        } else {
            start = null;
        }

        final QueryOptions options = QueryOptions.builder().tracing(params.tracing).build();

        final Filter filter = Tasks.setupFilter(filters, parser, params);
        final MetricBackend from = metric.useGroup(params.from);
        final MetricBackend to = metric.useGroup(params.to);
        final Semaphore migratePermits = new Semaphore(params.parallelism);

        return MetricBackends.keysPager(start, params.getLimit(), (s, l) -> from.keys(s, l, options), (set) -> {
            if (set.getTrace().isPresent()) {
                set.getTrace().get().formatTrace(io.out());
                io.out().flush();
            }
        }).directTransform(result -> {
            while (result.hasNext()) {
                final BackendKey next;

                synchronized (io) {
                    try {
                        next = result.next();
                    } catch (Exception e) {
                        io.out().println("Exception when pulling key: " + e);
                        e.printStackTrace(io.out());
                        continue;
                    }

                    if (!filter.apply(next.getSeries())) {
                        io.out().println(String.format("Skipping %s: does not match filter (%s)", next, filter));
                        continue;
                    }
                }

                final FutureDone<Void> report = new FutureDone<Void>() {
                    @Override
                    public void failed(Throwable cause) throws Exception {
                        synchronized (io) {
                            io.out().println("Migrate failed: " + next);
                            cause.printStackTrace(io.out());
                        }
                    }

                    @Override
                    public void resolved(Void result) throws Exception {
                        synchronized (io) {
                            io.out().println("Migrate successful: " + next);
                        }
                    }

                    @Override
                    public void cancelled() throws Exception {
                        synchronized (io) {
                            io.out().println("Migrate cancelled: " + next);
                        }
                    }
                };

                migratePermits.acquire();

                synchronized (io) {
                    io.out().println("Migrating: " + next);
                }

                from.fetchRow(next).lazyTransform(row -> {
                    return to.writeRow(next, row);
                }).onFinished(migratePermits::release).onDone(report).get();
            }

            return null;
        });
    }

    @ToString
    private static class Parameters extends Tasks.QueryParamsBase {
        @Option(name = "-f", aliases = { "--from" }, usage = "Backend group to load data from", metaVar = "<group>")
        private String from;

        @Option(name = "-t", aliases = { "--to" }, usage = "Backend group to load data to", metaVar = "<group>")
        private String to;

        @Option(name = "--start", usage = "First key to migrate", metaVar = "<json>")
        private String start;

        @Option(name = "--limit", usage = "Limit the number metadata entries to use (default: alot)")
        @Getter
        private int limit = Integer.MAX_VALUE;

        @Option(name = "--tracing", usage = "Trace the queries for more debugging when things go wrong")
        private boolean tracing = false;

        @Option(name = "--parallelism", usage = "The number of migration requests to send in parallel (default: 100)", metaVar = "<number>")
        private int parallelism = 100;

        @Argument
        @Getter
        private List<String> query = new ArrayList<String>();
    }
}
