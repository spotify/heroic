package com.spotify.heroic.common;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ThrowingTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private Runnable runnable;

    @Test
    public void testCall() throws Exception {
        thrown.expect(Exception.class);
        thrown.expectMessage("b");

        Throwing.call(() -> {
            throw new RuntimeException("a");
        }, () -> {
            throw new RuntimeException("b");
        });
    }

    @Test
    public void testRunOnce() throws Exception {
        thrown.expect(Exception.class);
        thrown.expectMessage("a");

        try {
            Throwing.call(() -> {
                throw new RuntimeException("a");
            }, runnable);
        } catch (final Exception e) {
            verify(runnable).run();
            throw e;
        }

        fail("Should not be reached");
    }
}
