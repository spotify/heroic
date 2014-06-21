package com.spotify.heroic.async;

import org.junit.Test;

public class ConcurrentCallbackTest extends
AbstractCallbackTest<ConcurrentCallback<Object>> {
    @Override
    protected ConcurrentCallback<Object> newCallback() {
        return new ConcurrentCallback<Object>();
    }

    @Test
    public void testDeadlockProtection() {
        final ConcurrentCallback<Object> callback = getCallback();
    }
}
