package com.spotify.heroic.metadata;

import java.util.List;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.injection.Lifecycle;
import com.spotify.heroic.metadata.model.DeleteTimeSeries;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metadata.model.FindTimeSeries;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.WriteResult;
import com.spotify.heroic.model.filter.Filter;
import com.spotify.heroic.statistics.MetadataBackendReporter;
import com.spotify.heroic.yaml.ConfigContext;
import com.spotify.heroic.yaml.ValidationException;

public interface MetadataBackend extends Lifecycle {
    public static interface YAML {
        MetadataBackend build(ConfigContext configContext,
                MetadataBackendReporter reporter) throws ValidationException;
    }

    public Callback<WriteResult> write(Series series)
            throws MetadataQueryException;

    public Callback<WriteResult> writeBatch(List<Series> series)
            throws MetadataQueryException;

    public Callback<FindTags> findTags(Filter filter)
            throws MetadataQueryException;

    public Callback<FindTimeSeries> findTimeSeries(Filter filter)
            throws MetadataQueryException;

    public Callback<DeleteTimeSeries> deleteTimeSeries(Filter filter)
            throws MetadataQueryException;

    public Callback<FindKeys> findKeys(Filter filter)
            throws MetadataQueryException;

    public Callback<Void> refresh();

    @Override
    public boolean isReady();
}