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
import com.spotify.heroic.http.model.TagsRequest;
import com.spotify.heroic.http.model.TagsResponse;
import com.spotify.heroic.http.model.TimeSeriesRequest;
import com.spotify.heroic.http.model.TimeSeriesResponse;

@Singleton
public class HeroicResourceCache {
    @Inject
    private TimeSeriesCache timeSeriesCache;

    public boolean isReady() {
        return timeSeriesCache.isReady();
    }

    final CachedHandle<TagsRequest, TagsResponse> tagsHandle = new CachedHandle<TagsRequest, TagsResponse>(
            new CachedRequest<TagsRequest, TagsResponse>() {
                @Override
                public TagsResponse run(TagsRequest query) {
                    final TimeSerieMatcher matcher = new FilteringTimeSerieMatcher(
                            query.getMatchKey(), query.getMatchTags(), query
                                    .getHasTags());

                    final FindTagsResult result = timeSeriesCache.findTags(
                            matcher, query.getInclude(), query.getExclude());

                    return new TagsResponse(result.getTags(), result.getSize());
                }
            });

    public TagsResponse tags(TagsRequest query) {
        return tagsHandle.run(query);
    }

    final CachedHandle<TimeSeriesRequest, KeysResponse> keysHandle = new CachedHandle<TimeSeriesRequest, KeysResponse>(
            new CachedRequest<TimeSeriesRequest, KeysResponse>() {
                @Override
                public KeysResponse run(TimeSeriesRequest query) {
                    final TimeSerieMatcher matcher = new FilteringTimeSerieMatcher(
                            query.getMatchKey(), query.getMatchTags(), query
                                    .getHasTags());

                    final FindKeysResult result = timeSeriesCache
                            .findKeys(matcher);

                    return new KeysResponse(result.getKeys(), result.getSize());
                }
            });

    public KeysResponse keys(TimeSeriesRequest query) {
        return keysHandle.run(query);
    }

    final CachedHandle<TimeSeriesRequest, TimeSeriesResponse> timeseriesHandle = new CachedHandle<TimeSeriesRequest, TimeSeriesResponse>(
            new CachedRequest<TimeSeriesRequest, TimeSeriesResponse>() {
                @Override
                public TimeSeriesResponse run(TimeSeriesRequest query) {
                    final TimeSerieMatcher matcher = new FilteringTimeSerieMatcher(
                            query.getMatchKey(), query.getMatchTags(), query
                                    .getHasTags());

                    final FindTimeSeriesResult result = timeSeriesCache
                            .findTimeSeries(matcher);

                    return new TimeSeriesResponse(result.getTimeSeries(),
                            result.getSize());
                }
            });

    public TimeSeriesResponse timeseries(TimeSeriesRequest query) {
        return timeseriesHandle.run(query);
    }

    public void refresh() {
        timeSeriesCache.refresh();
    }
}
