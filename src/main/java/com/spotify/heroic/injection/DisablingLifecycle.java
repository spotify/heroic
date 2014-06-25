package com.spotify.heroic.injection;

import java.util.concurrent.atomic.AtomicBoolean;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.codahale.metrics.Meter;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;

/**
 * A metric backend facade that learns about failures in the upstream backend
 * and 'disables' itself accordingly.
 *
 * @author udoprog
 */
@RequiredArgsConstructor
@Slf4j
public class DisablingLifecycle<T extends Lifecycle> implements Delegator<T>,
Lifecycle {
    private final T delegate;
    private final double threshold;
    private final long cooldownPeriod;

    private final AtomicBoolean disabled = new AtomicBoolean(false);
    private volatile long disabledAt = 0;

    private final Meter failMeter = new Meter();

    private final Callback.Handle<? extends Object> learner = new Callback.Handle<Object>() {
        @Override
        public void cancelled(CancelReason reason) throws Exception {
        }

        @Override
        public void failed(Exception e) throws Exception {
            checkFailures(e);
        }

        @Override
        public void resolved(Object result) throws Exception {
        }
    };

    @SuppressWarnings("unchecked")
    protected <C> Callback.Handle<C> learner() {
        return (Callback.Handle<C>) learner;
    }

    @Override
    public T delegate() {
        return delegate;
    }

    @Override
    public boolean isReady() {
        return !checkDisabled() && delegate.isReady();
    }

    @Override
    public void start() throws Exception {
        // on reload, backend no longer disabled.
        if (disabled.compareAndSet(true, false))
            disabledAt = 0;

        delegate.start();
    }

    @Override
    public void stop() throws Exception {
        delegate.stop();
    }

    protected boolean checkFailures(Exception e) {
        failMeter.mark();
        double rate = failMeter.getOneMinuteRate();

        if (rate > threshold) {
            if (disabled.compareAndSet(false, true)) {
                log.warn("Time for backend to cool down");
                disabledAt = System.currentTimeMillis();
                return true;
            }
        }

        return false;
    }

    protected boolean checkDisabled() {
        if (!disabled.get())
            return false;

        long period = System.currentTimeMillis() - disabledAt;

        if (period > cooldownPeriod) {
            if (disabled.compareAndSet(true, false)) {
                try {
                    delegate.stop();
                } catch (Exception e) {
                    log.warn("delegate stop failed", e);
                }

                try {
                    delegate.start();
                } catch (Exception e) {
                    log.warn("delegate start failed", e);
                }

                disabledAt = 0;
            }

            return false;
        }

        return true;
    }
}
