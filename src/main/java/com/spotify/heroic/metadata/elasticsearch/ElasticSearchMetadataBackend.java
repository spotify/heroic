package com.spotify.heroic.metadata.elasticsearch;

import java.util.Set;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.backend.BackendException;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.TimeSerieMatcher;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metadata.model.FindTimeSeries;
import com.spotify.heroic.statistics.MetadataBackendReporter;
import com.spotify.heroic.yaml.ValidationException;

@RequiredArgsConstructor
public class ElasticSearchMetadataBackend implements MetadataBackend {
    public static class YAML implements MetadataBackend.YAML {
        public static String TYPE = "!elasticsearch-metadata";

        @Override
        public MetadataBackend build(String context,
                MetadataBackendReporter reporter) throws ValidationException {
            return new ElasticSearchMetadataBackend(reporter);
        }
    }

    private final MetadataBackendReporter reporter;

    @Override
    public Callback<FindTags> findTags(TimeSerieMatcher matcher,
            Set<String> include, Set<String> exclude) throws BackendException {
        return null;
    }

    @Override
    public Callback<FindTimeSeries> findTimeSeries(TimeSerieMatcher matcher)
            throws BackendException {
        return null;
    }

    @Override
    public Callback<FindKeys> findKeys(TimeSerieMatcher matcher)
            throws BackendException {
        return null;
    }

    @Override
    public Callback<Void> refresh() {
        return null;
    }

    @Override
    public boolean isReady() {
        return true;
    }
}
