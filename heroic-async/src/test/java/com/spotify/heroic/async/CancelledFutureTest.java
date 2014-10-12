package com.spotify.heroic.async;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

@SuppressWarnings("unchecked")
public class CancelledFutureTest {
    private static final CancelReason reason = Mockito.mock(CancelReason.class);

    private FutureHandle<Integer> handle;

    @Before
    public void before() {
        handle = Mockito.mock(FutureHandle.class);
    }

    @After
    public void after() throws Exception {
        Mockito.verify(handle, Mockito.never()).resolved(Mockito.anyInt());
        Mockito.verify(handle, Mockito.never()).failed(Mockito.any(Exception.class));
        Mockito.verify(handle).cancelled(reason);
    }

    @Test
    public void testImmediatelyResolved() throws Exception {
        final Future<Integer> c = Futures.cancelled(reason);
        c.register(handle);
    }

    @Test
    public void testTransform() throws Exception {
        final Future<Boolean> c = Futures.cancelled(reason);
        final Transform<Boolean, Integer> transformer = Mockito.mock(Transform.class);
        Mockito.when(transformer.transform(true)).thenReturn(42);
        c.transform(transformer).register(handle);
    }

    @Test
    public void testDeferredTransform() throws Exception {
        final Future<Boolean> c = Futures.cancelled(reason);
        final DelayedTransform<Boolean, Integer> transformer = Mockito.mock(DelayedTransform.class);
        Mockito.when(transformer.transform(true)).thenReturn(Futures.resolved(42));
        c.transform(transformer).register(handle);
    }
}