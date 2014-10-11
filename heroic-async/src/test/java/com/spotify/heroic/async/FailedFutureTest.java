package com.spotify.heroic.async;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

@SuppressWarnings("unchecked")
public class FailedFutureTest {
    private static final Exception error = Mockito.mock(Exception.class);
    private FutureHandle<Integer> handle;

    @Before
    public void before() {
        handle = Mockito.mock(FutureHandle.class);
    }

    @After
    public void after() throws Exception {
        Mockito.verify(handle, Mockito.never()).resolved(42);
        Mockito.verify(handle).failed(error);
        Mockito.verify(handle, Mockito.never()).cancelled(Mockito.any(CancelReason.class));
    }

    @Test
    public void testImmediatelyResolved() throws Exception {
        final Future<Integer> c = Futures.failed(error);
        c.register(handle);
    }

    @Test
    public void testTransform() throws Exception {
        final Future<Boolean> c = Futures.failed(error);
        final Transformer<Boolean, Integer> transformer = Mockito.mock(Transformer.class);
        c.transform(transformer).register(handle);
        Mockito.verify(transformer, Mockito.never()).transform(Mockito.anyBoolean());
    }

    @Test
    public void testDeferredTransform() throws Exception {
        final Future<Boolean> c = Futures.failed(error);
        final DeferredTransformer<Boolean, Integer> transformer = Mockito.mock(DeferredTransformer.class);
        c.transform(transformer).register(handle);
        Mockito.verify(transformer, Mockito.never()).transform(Mockito.anyBoolean());
    }
}