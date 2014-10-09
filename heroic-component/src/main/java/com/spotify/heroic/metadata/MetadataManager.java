package com.spotify.heroic.metadata;

import java.util.Collection;
import java.util.List;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.model.DeleteSeries;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindSeries;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metric.model.WriteMetric;
import com.spotify.heroic.model.Series;

public interface MetadataManager {
    public List<MetadataBackend> getBackends();

    public Callback<FindTags> findTags(final Filter filter);

    public Callback<String> bufferWrite(WriteMetric write);

    public Callback<String> bufferWrite(Series series);

    public Callback<List<String>> bufferWrites(Collection<WriteMetric> writes);

    public Callback<FindSeries> findSeries(final Filter filter);

    public Callback<DeleteSeries> deleteSeries(final Filter filter);

    public Callback<FindKeys> findKeys(final Filter filter);

    public Callback<Boolean> refresh();

    public boolean isReady();
}
