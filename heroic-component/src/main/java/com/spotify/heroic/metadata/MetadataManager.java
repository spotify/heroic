package com.spotify.heroic.metadata;

import java.util.Collection;
import java.util.List;

import com.spotify.heroic.async.Future;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.model.DeleteSeries;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindSeries;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metric.model.WriteMetric;
import com.spotify.heroic.model.Series;

public interface MetadataManager {
    public List<MetadataBackend> getBackends();

    public Future<FindTags> findTags(final Filter filter);

    public Future<String> bufferWrite(WriteMetric write);

    public Future<String> bufferWrite(Series series);

    public Future<List<String>> bufferWrites(Collection<WriteMetric> writes);

    public Future<FindSeries> findSeries(final Filter filter);

    public Future<DeleteSeries> deleteSeries(final Filter filter);

    public Future<FindKeys> findKeys(final Filter filter);

    public Future<Boolean> refresh();

    public boolean isReady();
}
