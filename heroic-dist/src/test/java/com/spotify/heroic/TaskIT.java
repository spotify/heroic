package com.spotify.heroic;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.ingestion.Ingestion;
import com.spotify.heroic.ingestion.IngestionComponent;
import com.spotify.heroic.ingestion.IngestionManager;
import com.spotify.heroic.ingestion.WriteOptions;
import com.spotify.heroic.shell.ShellIO;
import eu.toolchain.async.AsyncFuture;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.spotify.heroic.test.Data.points;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;

public class TaskIT extends AbstractLocalClusterIT {
    private final Series s1 = Series.of("key1", ImmutableMap.of("shared", "a", "diff", "a"));
    private final Series s2 = Series.of("key1", ImmutableMap.of("shared", "a", "diff", "b"));

    private ShellTasks tasks;

    private PrintWriter out;
    private ShellIO io;

    @Override
    protected List<URI> instanceUris() {
        return ImmutableList.of(URI.create("jvm://a"));
    }

    @Before
    public void setup() {
        tasks = instances.get(0).inject(CoreComponent::tasks);

        io = mock(ShellIO.class);
        out = mock(PrintWriter.class);
        doReturn(out).when(io).out();
    }

    @Override
    protected AsyncFuture<Void> prepareEnvironment() {
        final List<IngestionManager> ingestion = instances
            .stream()
            .map(i -> i.inject(IngestionComponent::ingestionManager))
            .collect(Collectors.toList());

        final List<AsyncFuture<Ingestion>> writes = new ArrayList<>();

        final IngestionManager m1 = ingestion.get(0);

        writes.add(m1
            .useDefaultGroup()
            .write(new Ingestion.Request(WriteOptions.defaults(), s1,
                points().p(10, 1D).p(30, 2D).build())));

        return async.collectAndDiscard(writes);
    }

    @Test
    public void readFile() throws Exception {
        final InputStream in = new ByteArrayInputStream("foo\nbar\n".getBytes(Charsets.UTF_8));

        doReturn(in).when(io).newInputStream(any(Path.class), eq(StandardOpenOption.READ));

        run("test-read-file");

        final InOrder order = inOrder(out);

        order.verify(out).println("Read: 0");
        order.verify(out).println("Read: 1");
        order.verify(out, never()).println(any(String.class));
    }

    @Test
    public void print() {
        run("test-print", "--count", "2");

        final InOrder order = inOrder(out);

        order.verify(out).println("Count: 00000000");
        order.verify(out).println("Count: 00000001");
        order.verify(out, never()).println(any(String.class));
    }

    @Test
    public void pause() {
        run("pause");

        final InOrder order = inOrder(out);

        order.verify(out).println("Pausing Consumers");
        order.verify(out, never()).println(any(String.class));
    }

    @Test
    public void resume() {
        run("resume");

        final InOrder order = inOrder(out);

        order.verify(out).println("Resuming Consumers");
        order.verify(out, never()).println(any(String.class));
    }

    @Test
    public void parseQuery() {
        run("parse-query", "*");

        final InOrder order = inOrder(out);

        order.verify(out).println(any(String.class));
        order.verify(out, never()).println(any(String.class));
    }

    @Test
    public void parseQueryEval() {
        run("parse-query", "--eval", "*");

        final InOrder order = inOrder(out);

        order.verify(out).println(any(String.class));
        order.verify(out, never()).println(any(String.class));
    }

    @Test
    public void ingestionFilter() {
        run("ingestion-filter");
        run("ingestion-filter", "role=lol");
        run("ingestion-filter");

        final InOrder order = inOrder(out);

        order.verify(out).println("Current ingestion filter: [true]");
        order.verify(out).println("Updating ingestion filter to: [=, role, lol]");
        order.verify(out).println("Current ingestion filter: [=, role, lol]");
        order.verify(out, never()).println(any(String.class));
    }

    @Test
    public void refresh() {
        run("refresh");

        final InOrder order = inOrder(out);

        order.verify(out, never()).println(any(String.class));
    }

    private void run(final String... command) {
        try {
            tasks.evaluate(ImmutableList.copyOf(command), io).get();
        } catch (final Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
