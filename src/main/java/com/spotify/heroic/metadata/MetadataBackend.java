package com.spotify.heroic.metadata;

import java.util.Set;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metadata.model.FindTimeSeries;
import com.spotify.heroic.metadata.model.TimeSerieQuery;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.WriteResponse;
import com.spotify.heroic.statistics.MetadataBackendReporter;
import com.spotify.heroic.yaml.ValidationException;

public interface MetadataBackend {
    public static interface YAML {
        MetadataBackend build(String context, MetadataBackendReporter reporter)
                throws ValidationException;
    }

    public Callback<WriteResponse> write(TimeSerie timeSerie)
            throws MetadataQueryException;

    public Callback<FindTags> findTags(TimeSerieQuery matcher,
            Set<String> include, Set<String> exclude)
            throws MetadataQueryException;

    public Callback<FindTimeSeries> findTimeSeries(TimeSerieQuery matcher)
            throws MetadataQueryException;

    public Callback<FindKeys> findKeys(TimeSerieQuery matcher)
            throws MetadataQueryException;

    public Callback<Void> refresh();

    public boolean isReady();
}