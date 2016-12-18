package com.spotify.heroic.shell;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.shell.protocol.Command;
import eu.toolchain.async.AsyncFuture;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Optional;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class TaskSpecificationTest {
    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Mock
    public TaskParameters params;

    @Mock
    public AsyncFuture<Void> future;

    @Test
    public void testNameMissing() {
        class TestNameMissing extends AbstractShellTask {
        }

        expected.expect(IllegalArgumentException.class);
        TaskSpecification.fromClass(TestNameMissing.class);
    }

    @Test
    public void testName() {
        @TaskName(value = "task", aliases = "other")
        class TestName extends AbstractShellTask {
        }

        final TaskSpecification spec = TaskSpecification.fromClass(TestName.class);
        assertEquals("task", spec.getName());
        assertEquals(ImmutableList.of("other"), spec.getAliases());
    }

    @Test
    public void testMissingUsage() {
        @TaskName("task")
        class TestMissingUsage extends AbstractShellTask {
        }

        final TaskSpecification spec = TaskSpecification.fromClass(TestMissingUsage.class);
        assertEquals("task", spec.getName());
        assertEquals("<no @ShellTaskUsage annotation for null>", spec.getUsage());
    }

    @Test
    public void testUsage() {
        @TaskName("task")
        @TaskUsage("usage")
        class TestUsage extends AbstractShellTask {
        }

        final TaskSpecification spec = TaskSpecification.fromClass(TestUsage.class);
        assertEquals("task", spec.getName());
        assertEquals("usage", spec.getUsage());
    }

    @Test
    public void testOptions() {
        @TaskName("task")
        @TaskParametersModel(TestUsage.Parameters.class)
        class TestUsage extends AbstractShellTask {
            class Parameters {
                @Option(name = "--limit", metaVar = "<number>", usage = "limit usage")
                public int limit = 10;

                @Option(name = "--flag")
                public boolean flag = false;

                @Argument(metaVar = "<file>", required = true)
                public String argument1;

                @Argument(metaVar = "<file|->", required = true)
                public String argument2;

                @Argument(metaVar = "<group>")
                public String argument3;
            }
        }

        final Command.Opt limit =
            new Command.Opt("--limit", ImmutableList.of(), new Command.Type.Number(),
                Optional.of("limit"), Optional.of("limit usage"), false, true);

        final Command.Opt flag =
            new Command.Opt("--flag", ImmutableList.of(), new Command.Type.Any(),
                Optional.empty(), Optional.empty(), false, true);

        final Command.Arg argument1 =
            new Command.Arg("argument1", new Command.Type.File(), false, false);

        final Command.Arg argument2 =
            new Command.Arg("argument2", new Command.Type.FileOrStd(), false, false);

        final Command.Arg argument3 =
            new Command.Arg("argument3", new Command.Type.Group(), false, true);

        final TaskSpecification spec = TaskSpecification.fromClass(TestUsage.class);
        assertEquals("task", spec.getName());
        assertEquals(ImmutableList.of(limit, flag), spec.getOptions());
        assertEquals(ImmutableList.of(argument1, argument2, argument3), spec.getArguments());
    }

    abstract class AbstractShellTask implements ShellTask {
        @Override
        public TaskParameters params() {
            return params;
        }

        @Override
        public AsyncFuture<Void> run(
            final ShellIO io, final TaskParameters params
        ) throws Exception {
            return future;
        }
    }
}
