package com.spotify.heroic.async;

import java.util.concurrent.TimeUnit;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.codahale.metrics.Timer;

@Slf4j
@RequiredArgsConstructor
public abstract class CallbackHandle<T, C> implements Callback.Handle<T> {
    private final String task;
    private final Timer timer;
    private final Callback<C> callback;

    @Override
    public void cancel(CancelReason reason) throws Exception {
        callback.cancel(reason);
    }

    @Override
    public void error(Throwable e) throws Exception {
        callback.fail(e);
    }

    @Override
    public void finish(T result)
            throws Exception {
        if (!callback.isInitialized())
            return;

        final Timer.Context context = timer.time();
        log.debug("{} (id {}, started)", task, hashCode());

        try {
            execute(callback, result);
        } catch(Throwable t) {
            callback.fail(t);
            return;
        } finally {
            final long time = context.stop();
            final long ms = TimeUnit.MILLISECONDS.convert(time,
                    TimeUnit.NANOSECONDS);
            log.debug("{} (id {}, ended: {}ms)", task, hashCode(), ms);
        }
    }

    public abstract void execute(Callback<C> callback, T result) throws Exception;
}
