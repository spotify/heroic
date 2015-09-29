package com.spotify.heroic.shell.task;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.kohsuke.args4j.Option;

import com.google.inject.Inject;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.ToString;

@TaskUsage("Test to read and write a file")
@TaskName("read-write-test")
public class ReadWriteTest implements ShellTask {
    @Inject
    private AsyncFramework async;

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, final TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final Path p = Paths.get(params.file);

        try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(io.newOutputStream(p)))) {
            writer.write("Hello World\n");
        }

        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(io.newInputStream(p)))) {
            io.out().println(reader.readLine());
        }

        io.out().flush();
        return async.resolved();
    }

    @ToString
    private static class Parameters extends AbstractShellTaskParams {
        @Option(name = "-f", aliases = {"--file"}, usage = "File to perform test against", metaVar = "<file>")
        private String file = null;
    }
}