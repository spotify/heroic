package com.spotify.heroic.metadata;

import java.util.Set;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.backend.BackendException;
import com.spotify.heroic.backend.TimeSerieMatcher;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metadata.model.FindTimeSeries;

public interface MetadataBackend {
    public Callback<FindTags> findTags(TimeSerieMatcher matcher, Set<String> include, Set<String> exclude) throws BackendException;
    public Callback<FindTimeSeries> findTimeSeries(TimeSerieMatcher matcher) throws BackendException;
    public Callback<FindKeys> findKeys(TimeSerieMatcher matcher) throws BackendException;
    public void refresh();
    public boolean isReady();
}