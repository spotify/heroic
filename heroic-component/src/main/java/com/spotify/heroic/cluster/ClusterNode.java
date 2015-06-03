package com.spotify.heroic.cluster;

import java.util.List;

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.model.CountSeries;
import com.spotify.heroic.metadata.model.DeleteSeries;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindSeries;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metric.model.ResultGroups;
import com.spotify.heroic.metric.model.WriteMetric;
import com.spotify.heroic.metric.model.WriteResult;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.RangeFilter;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.TimeData;
import com.spotify.heroic.suggest.model.KeySuggest;
import com.spotify.heroic.suggest.model.MatchOptions;
import com.spotify.heroic.suggest.model.TagKeyCount;
import com.spotify.heroic.suggest.model.TagSuggest;
import com.spotify.heroic.suggest.model.TagValueSuggest;
import com.spotify.heroic.suggest.model.TagValuesSuggest;

import eu.toolchain.async.AsyncFuture;

public interface ClusterNode {
    public NodeMetadata metadata();

    public AsyncFuture<Void> close();

    public Group useGroup(String group);

    public interface Group {
        public AsyncFuture<ResultGroups> query(Class<? extends TimeData> source, Filter filter, List<String> groupBy,
                DateRange range, Aggregation aggregation, boolean disableCache);

        public AsyncFuture<FindTags> findTags(RangeFilter filter);

        public AsyncFuture<FindKeys> findKeys(RangeFilter filter);

        public AsyncFuture<FindSeries> findSeries(RangeFilter filter);

        public AsyncFuture<DeleteSeries> deleteSeries(RangeFilter filter);

        public AsyncFuture<CountSeries> countSeries(RangeFilter filter);

        public AsyncFuture<TagKeyCount> tagKeyCount(RangeFilter filter);

        public AsyncFuture<TagSuggest> tagSuggest(RangeFilter filter, MatchOptions options, String key, String value);

        public AsyncFuture<KeySuggest> keySuggest(RangeFilter filter, MatchOptions options, String key);

        public AsyncFuture<TagValuesSuggest> tagValuesSuggest(RangeFilter filter, List<String> exclude, int groupLimit);

        public AsyncFuture<TagValueSuggest> tagValueSuggest(RangeFilter filter, String key);

        public AsyncFuture<WriteResult> writeSeries(DateRange range, Series series);

        public AsyncFuture<WriteResult> writeMetric(WriteMetric write);
    }
}
