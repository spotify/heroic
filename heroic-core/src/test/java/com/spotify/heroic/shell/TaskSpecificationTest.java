package com.spotify.heroic.shell;

import com.google.common.collect.ImmutableList;
import eu.toolchain.async.AsyncFuture;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

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
