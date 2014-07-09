package com.spotify.heroic.metadata;

import java.util.List;
import java.util.Set;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.injection.DisablingLifecycle;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metadata.model.FindTimeSeries;
import com.spotify.heroic.metadata.model.TimeSerieQuery;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.WriteResponse;

/**
 * A metric backend facade that learns about failures in the upstream backend
 * and 'disables' itself accordingly.
 * 
 * @author udoprog
 */
public class DisablingMetadataBackend extends
DisablingLifecycle<MetadataBackend> implements MetadataBackend {
    public DisablingMetadataBackend(MetadataBackend delegate, double threshold,
            long cooldownPeriod) {
        super(delegate, threshold, cooldownPeriod);
    }

    @Override
    public Callback<WriteResponse> write(TimeSerie timeSerie)
            throws MetadataQueryException {
        return delegate().write(timeSerie).register(
                this.<WriteResponse> learner());
    }

    @Override
    public Callback<WriteResponse> writeBatch(List<TimeSerie> timeSeries)
            throws MetadataQueryException {
        return delegate().writeBatch(timeSeries).register(
                this.<WriteResponse> learner());
    }

    @Override
    public Callback<FindTags> findTags(TimeSerieQuery matcher,
            Set<String> include, Set<String> exclude)
                    throws MetadataQueryException {
        return delegate().findTags(matcher, include, exclude).register(
                this.<FindTags> learner());
    }

    @Override
    public Callback<FindTimeSeries> findTimeSeries(TimeSerieQuery matcher)
            throws MetadataQueryException {
        return delegate().findTimeSeries(matcher).register(
                this.<FindTimeSeries> learner());
    }

    @Override
    public Callback<FindKeys> findKeys(TimeSerieQuery matcher)
            throws MetadataQueryException {
        return delegate().findKeys(matcher).register(this.<FindKeys> learner());
    }

    @Override
    public Callback<Void> refresh() {
        return delegate().refresh().register(this.<Void> learner());
    }
}
