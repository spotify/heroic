package com.spotify.heroic.async;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

public class CallbackGroupHandleTest {
    final List<Void> results = new LinkedList<Void>();
    final List<Throwable> exceptions = new LinkedList<Throwable>();

    @Test
    public void testFinish() throws Exception {
        final Object reference = new Object();
        Callback<Object> callback = mock(Callback.class);
        when(callback.isInitialized()).thenReturn(true);

        CallbackGroupHandle<Object, Void> group = new CallbackGroupHandle<Object, Void>(
                callback) {

            @Override
            public Object execute(Collection<Void> results,
                    Collection<Throwable> errors, int cancelled)
                    throws Exception {
                return reference;
            }
        };

        group.done(results, exceptions, 0);
        verify(callback).finish(reference);
    }

    @Test
    public void testThrows() throws Exception {
        final Exception reference = new Exception();
        Callback<Object> callback = mock(Callback.class);
        when(callback.isInitialized()).thenReturn(true);

        CallbackGroupHandle<Object, Void> group = new CallbackGroupHandle<Object, Void>(
                callback) {

            @Override
            public Object execute(Collection<Void> results,
                    Collection<Throwable> errors, int cancelled)
                    throws Exception {
                throw reference;
            }
        };

        group.done(results, exceptions, 0);
        verify(callback).fail(reference);
    }
}
