package com.spotify.heroic.metadata;

import java.util.Set;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.backend.BackendException;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metadata.model.FindTimeSeries;
import com.spotify.heroic.statistics.MetadataBackendReporter;
import com.spotify.heroic.yaml.ValidationException;

public interface MetadataBackend {
    public static interface YAML {
        MetadataBackend build(String context, MetadataBackendReporter reporter) throws ValidationException;
    }

    public Callback<FindTags> findTags(TimeSerieMatcher matcher, Set<String> include, Set<String> exclude) throws BackendException;
    public Callback<FindTimeSeries> findTimeSeries(TimeSerieMatcher matcher) throws BackendException;
    public Callback<FindKeys> findKeys(TimeSerieMatcher matcher) throws BackendException;
    public Callback<Void> refresh();
    public boolean isReady();
}