package com.spotify.heroic.async;

import lombok.Getter;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Abstract tests that any Callback implementation have to pass.
 * 
 * Use by extending in your own test class.
 * 
 * @author udoprog
 * @param <T>
 *            Type of the callback implementation to test.
 */
public abstract class AbstractCallbackTest<T extends Callback<Object>> {
    private static final Object REFERENCE = new Object();
    private static final Exception ERROR = Mockito.mock(Exception.class);

    @Getter
    private Callback.Handle<Object> handle;

    @Getter
    private T callback;

    @SuppressWarnings("unchecked")
    @Before
    public void before() {
        handle = Mockito.mock(Callback.Handle.class);
        callback = newCallback();
    }

    abstract protected T newCallback();

    @Test
    public void testShouldFireCallbacks() throws Exception {
        callback.register(handle);
        callback.resolve(REFERENCE);

        Mockito.verify(handle, Mockito.never()).failed(
                Mockito.any(Exception.class));
        Mockito.verify(handle, Mockito.never()).cancelled(
                Mockito.any(CancelReason.class));
        Mockito.verify(handle).resolved(REFERENCE);

        callback.resolve(REFERENCE);

        Mockito.verify(handle, Mockito.never()).failed(
                Mockito.any(Exception.class));
        Mockito.verify(handle, Mockito.never()).cancelled(
                Mockito.any(CancelReason.class));
        Mockito.verify(handle).resolved(REFERENCE);
    }

    @Test
    public void testShouldFireCallbacksAfterResolve() throws Exception {
        callback.resolve(REFERENCE);
        callback.register(handle);

        Mockito.verify(handle, Mockito.never()).failed(
                Mockito.any(Exception.class));
        Mockito.verify(handle, Mockito.never()).cancelled(
                Mockito.any(CancelReason.class));
        Mockito.verify(handle).resolved(REFERENCE);

        // attempt a second register.
        callback.register(handle);

        // handle should have been called again.
        Mockito.verify(handle, Mockito.never()).failed(
                Mockito.any(Exception.class));
        Mockito.verify(handle, Mockito.never()).cancelled(
                Mockito.any(CancelReason.class));
        Mockito.verify(handle, Mockito.atLeast(2)).resolved(REFERENCE);
    }

    @Test
    public void testShouldFireFailure() throws Exception {
        callback.register(handle);
        callback.fail(ERROR);

        Mockito.verify(handle).failed(ERROR);
        Mockito.verify(handle, Mockito.never()).cancelled(
                Mockito.any(CancelReason.class));
        Mockito.verify(handle, Mockito.never()).resolved(Mockito.any());

        // attempt a second fail.
        callback.fail(ERROR);

        // should be same state.
        Mockito.verify(handle).failed(ERROR);
        Mockito.verify(handle, Mockito.never()).cancelled(
                Mockito.any(CancelReason.class));
        Mockito.verify(handle, Mockito.never()).resolved(Mockito.any());
    }
}
