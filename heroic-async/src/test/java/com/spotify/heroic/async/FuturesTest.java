package com.spotify.heroic.async;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class FuturesTest {
    @Test
    public void testDeferredTransformResolved() throws Exception {
        final Future<Object> future = Mockito.mock(Future.class);
        final DelayedTransform<Object, Object> transformer = Mockito.mock(DelayedTransform.class);
        final Future<Object> target = Mockito.mock(Future.class);
        final Future<Object> transform = Mockito.mock(Future.class);
        final Object result = new Object();
        final Object targetResult = new Object();

        Mockito.when(transformer.transform(result)).thenReturn(transform);

        ArgumentCaptor<FutureHandle> register1 = ArgumentCaptor.forClass(FutureHandle.class);

        Futures.transform(future, transformer, target);
        Mockito.verify(future).register(register1.capture());

        register1.getValue().resolved(result);

        // after 'future' has been resolved, the transformer will have returned another future that will be monitored
        // for changes.
        ArgumentCaptor<FutureHandle> register2 = ArgumentCaptor.forClass(FutureHandle.class);
        Mockito.verify(transform).register(register2.capture());

        register2.getValue().resolved(targetResult);

        Mockito.verify(target, Mockito.times(2)).register(Mockito.any(FutureHandle.class));
        Mockito.verify(target).resolve(targetResult);
    }

    @Test
    public void testTransformResolved() throws Exception {
        final Future<Object> future = Mockito.mock(Future.class);
        final Transform<Object, Object> transformer = Mockito.mock(Transform.class);
        final Future<Object> target = Mockito.mock(Future.class);
        final Object result = new Object();

        Mockito.when(transformer.transform(result)).thenReturn(result);

        ArgumentCaptor<FutureHandle> register = ArgumentCaptor.forClass(FutureHandle.class);

        Futures.transform(future, transformer, null, target);
        Mockito.verify(future).register(register.capture());

        register.getValue().resolved(result);

        Mockito.verify(target, Mockito.times(1)).register(Mockito.any(FutureHandle.class));
        Mockito.verify(target).resolve(result);
    }
}
