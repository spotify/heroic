package com.spotify.heroic.shell;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ShellTasks {
    final SortedMap<String, ShellTask> tasks;
    final AsyncFramework async;

    public AsyncFuture<Void> evaluate(List<String> command, ShellIO io) throws Exception {
        if (command.isEmpty()) {
            return async.failed(new Exception("Empty command"));
        }

        final String taskName = command.iterator().next();
        final List<String> args = command.subList(1, command.size());

        final ShellTask task = resolveTask(io.out(), taskName);

        if (task == null) {
            return async.failed(new Exception("No task matching: " + taskName));
        }

        final TaskParameters params = task.params();

        if (params != null) {
            final CmdLineParser parser = new CmdLineParser(params);

            try {
                parser.parseArgument(args);
            } catch (CmdLineException e) {
                return async.failed(e);
            }

            if (params.help()) {
                parser.printUsage(io.out(), null);
                return async.resolved();
            }
        }

        try {
            return task.run(io, params);
        } catch(Exception e) {
            return async.failed(e);
        }
    }

    ShellTask resolveTask(final PrintWriter out, final String taskName) {
        final SortedMap<String, ShellTask> selected = tasks.subMap(taskName, taskName
                + Character.MAX_VALUE);

        final ShellTask exact;

        // exact match
        if ((exact = selected.get(taskName)) != null) {
            return exact;
        }

        // no fuzzy matches
        if (selected.isEmpty()) {
            out.println("No such task '" + taskName + "'");
            return null;
        }

        if (selected.size() > 1) {
            out.println(String.format("Too many (%d) matching tasks:", selected.size()));

            for (final Map.Entry<String, ShellTask> e : tasks.entrySet()) {
                out.println(String.format("  %s", e.getKey()));
            }

            return null;
        }

        return selected.values().iterator().next();
    }
}