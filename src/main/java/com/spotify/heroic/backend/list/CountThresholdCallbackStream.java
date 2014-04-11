package com.spotify.heroic.backend.list;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CallbackStream;
import com.spotify.heroic.async.CancelReason;

/**
 * A callback stream implementation that receives result from a list of
 * callbacks.
 * 
 * @author udoprog
 */
@Slf4j
final class CountThresholdCallbackStream implements CallbackStream.Handle<Long> {
    private final long threshold;
    private final Callback<Void> callback;

    private final AtomicLong totalColumnCount = new AtomicLong(0);
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    CountThresholdCallbackStream(long threshold, Callback<Void> callback) {
        this.threshold = threshold;
        this.callback = callback;
    }

    @Override
    public void finish(CallbackStream<Long> stream, Callback<Long> callback,
            Long result) throws Exception {
        final long currentValue = totalColumnCount.addAndGet(result);

        if (currentValue > threshold) {
            if (cancelled.compareAndSet(false, true)) {
                // TODO: this should not be necessary since we are cancelling
                // the callback we should be able to register on it!
                stream.cancel(new CancelReason("Counter threshold reached"));
                this.callback.cancel(new CancelReason(
                        "Counter threshold reached"));
            }
        }
    }

    @Override
    public void error(CallbackStream<Long> stream, Callback<Long> callback,
            Throwable error) throws Exception {
    }

    @Override
    public void cancel(CallbackStream<Long> stream, Callback<Long> callback,
            CancelReason reason) throws Exception {
    }

    @Override
    public void done(int successful, int failed, int cancelled)
            throws Exception {
        if (this.cancelled.get()) {
            log.warn("Request cancelled");
            return;
        }

        this.callback.finish(null);
    }
}