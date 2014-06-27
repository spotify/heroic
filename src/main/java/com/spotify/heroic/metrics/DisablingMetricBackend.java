package com.spotify.heroic.metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.CancelledCallback;
import com.spotify.heroic.injection.DisablingLifecycle;
import com.spotify.heroic.metrics.model.FetchDataPoints;
import com.spotify.heroic.metrics.model.FetchDataPoints.Result;
import com.spotify.heroic.metrics.model.FindTimeSeries;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.WriteEntry;
import com.spotify.heroic.model.WriteResponse;

/**
 * A metric backend facade that learns about failures in the upstream backend
 * and 'disables' itself accordingly.
 *
 * @author udoprog
 */
@Slf4j
public class DisablingMetricBackend extends DisablingLifecycle<MetricBackend>
        implements MetricBackend {
    private static final Callback<? extends Object> DISABLED = new CancelledCallback<Object>(
            CancelReason.BACKEND_DISABLED);

    public DisablingMetricBackend(MetricBackend delegate, double threshold,
            long cooldownPeriod) {
        super(delegate, threshold, cooldownPeriod);
    }

    @Override
    public TimeSerie getPartition() {
        return delegate().getPartition();
    }

    @Override
    public boolean matchesPartition(TimeSerie timeSerie) {
        return delegate().matchesPartition(timeSerie);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Callback<WriteResponse> write(WriteEntry write) {
        if (checkDisabled())
            return (Callback<WriteResponse>) DISABLED;

        return delegate().write(write).register(this.<WriteResponse> learner());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Callback<WriteResponse> write(Collection<WriteEntry> writes) {
        if (checkDisabled())
            return (Callback<WriteResponse>) DISABLED;

        return delegate().write(writes)
                .register(this.<WriteResponse> learner());
    }

    @Override
    public List<Callback<FetchDataPoints.Result>> query(TimeSerie timeSerie,
            DateRange range) {
        if (checkDisabled())
            return new ArrayList<Callback<FetchDataPoints.Result>>();

        final List<Callback<Result>> callbacks = delegate().query(timeSerie,
                range);

        final Callback.Handle<FetchDataPoints.Result> learner = new Callback.Handle<FetchDataPoints.Result>() {
            @Override
            public void cancelled(CancelReason reason) throws Exception {
            }

            @Override
            public void failed(Exception e) throws Exception {
                if (checkFailures(e)) {
                    log.warn("Disabling backends");

                    for (final Callback<FetchDataPoints.Result> callback : callbacks) {
                        callback.cancel(CancelReason.BACKEND_DISABLED);
                    }
                }
            }

            @Override
            public void resolved(Result result) throws Exception {
            }
        };

        for (final Callback<Result> callback : callbacks) {
            callback.register(learner);
        }

        return callbacks;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Callback<Set<TimeSerie>> getAllTimeSeries() {
        if (checkDisabled())
            return (Callback<Set<TimeSerie>>) DISABLED;

        return delegate().getAllTimeSeries().register(
                this.<Set<TimeSerie>> learner());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Callback<Long> getColumnCount(TimeSerie timeSerie, DateRange range) {
        if (checkDisabled())
            return (Callback<Long>) DISABLED;

        return delegate().getColumnCount(timeSerie, range).register(
                this.<Long> learner());
    }
}
