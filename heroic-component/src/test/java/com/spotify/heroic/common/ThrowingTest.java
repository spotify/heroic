package com.spotify.heroic.common;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

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
            throw new Exception("a");
        } , () -> {
            throw new Exception("b");
        });
    }

    @Test
    public void testRunOnce() throws Exception {
        thrown.expect(Exception.class);
        thrown.expectMessage("a");

        try {
            Throwing.call(() -> {
                throw new Exception("a");
            } , runnable::run);
        } catch (final Exception e) {
            verify(runnable).run();
            throw e;
        }

        fail("Should not be reached");
    }
}
