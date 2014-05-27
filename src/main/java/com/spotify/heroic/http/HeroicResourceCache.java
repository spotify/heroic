package com.spotify.heroic.http;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.spotify.heroic.backend.FilteringTimeSerieMatcher;
import com.spotify.heroic.backend.TimeSerieMatcher;
import com.spotify.heroic.backend.TimeSeriesCache;
import com.spotify.heroic.backend.TimeSeriesCache.FindKeysResult;
import com.spotify.heroic.backend.TimeSeriesCache.FindTagsResult;
import com.spotify.heroic.backend.TimeSeriesCache.FindTimeSeriesResult;
import com.spotify.heroic.http.cache.CachedHandle;
import com.spotify.heroic.http.cache.CachedRequest;
import com.spotify.heroic.http.model.KeysResponse;
import com.spotify.heroic.http.model.TagsQuery;
import com.spotify.heroic.http.model.TagsResponse;
import com.spotify.heroic.http.model.TimeSeriesQuery;
import com.spotify.heroic.http.model.TimeSeriesResponse;

@Singleton
public class HeroicResourceCache {
    @Inject
    private TimeSeriesCache timeSeriesCache;

    public boolean isReady() {
        return timeSeriesCache.isReady();
    }

    final CachedHandle<TagsQuery, TagsResponse> tagsHandle = new CachedHandle<TagsQuery, TagsResponse>(
            new CachedRequest<TagsQuery, TagsResponse>() {
                @Override
                public TagsResponse run(TagsQuery query) {
                    final TimeSerieMatcher matcher = new FilteringTimeSerieMatcher(
                            query.getMatchKey(), query.getMatchTags(), query
                                    .getHasTags());

                    final FindTagsResult result = timeSeriesCache.findTags(
                            matcher, query.getInclude(), query.getExclude());

                    return new TagsResponse(result.getTags(), result.getSize());
                }
            });

    public TagsResponse tags(TagsQuery query) {
        return tagsHandle.run(query);
    }

    final CachedHandle<TimeSeriesQuery, KeysResponse> keysHandle = new CachedHandle<TimeSeriesQuery, KeysResponse>(
            new CachedRequest<TimeSeriesQuery, KeysResponse>() {
                @Override
                public KeysResponse run(TimeSeriesQuery query) {
                    final TimeSerieMatcher matcher = new FilteringTimeSerieMatcher(
                            query.getMatchKey(), query.getMatchTags(), query
                                    .getHasTags());

                    final FindKeysResult result = timeSeriesCache
                            .findKeys(matcher);

                    return new KeysResponse(result.getKeys(), result.getSize());
                }
            });

    public KeysResponse keys(TimeSeriesQuery query) {
        return keysHandle.run(query);
    }

    final CachedHandle<TimeSeriesQuery, TimeSeriesResponse> timeseriesHandle = new CachedHandle<TimeSeriesQuery, TimeSeriesResponse>(
            new CachedRequest<TimeSeriesQuery, TimeSeriesResponse>() {
                @Override
                public TimeSeriesResponse run(TimeSeriesQuery query) {
                    final TimeSerieMatcher matcher = new FilteringTimeSerieMatcher(
                            query.getMatchKey(), query.getMatchTags(), query
                                    .getHasTags());

                    final FindTimeSeriesResult result = timeSeriesCache
                            .findTimeSeries(matcher);

                    return new TimeSeriesResponse(result.getTimeSeries(),
                            result.getSize());
                }
            });

    public TimeSeriesResponse timeseries(TimeSeriesQuery query) {
        return timeseriesHandle.run(query);
    }

    public void refresh() {
        timeSeriesCache.refresh();
    }
}
