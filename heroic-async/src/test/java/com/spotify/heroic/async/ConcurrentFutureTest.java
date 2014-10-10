package com.spotify.heroic.async;

import org.junit.Assert;
import org.junit.Test;

public class ConcurrentFutureTest extends
        AbstractFutureTest<ConcurrentFuture<Object>> {
    private static final Object REFERENCE = new Object();

    @Override
    protected ConcurrentFuture<Object> newCallback() {
        return new ConcurrentFuture<Object>();
    }

    @Test
    public void testDeadlockProtection() {
        // concurrent callback uses synchronized blocks and defers some actions
        // to outside of those blocks to avoid deadlocks.
        // This situation used to instigate a deadlock.
        final ConcurrentFuture<Object> c1 = new ConcurrentFuture<Object>();
        final ConcurrentFuture<Object> c2 = new ConcurrentFuture<Object>();
        c1.register(c2);
        c2.register(c1);
        c1.resolve(REFERENCE);
        Assert.assertFalse(c1.isReady());
        Assert.assertFalse(c2.isReady());
    }
}
