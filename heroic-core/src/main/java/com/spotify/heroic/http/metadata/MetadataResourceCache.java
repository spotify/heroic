package com.spotify.heroic.http.metadata;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import lombok.Data;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.spotify.heroic.metadata.ClusteredMetadataManager;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.model.RangeFilter;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureDone;

public class MetadataResourceCache {
    @Inject
    private ClusteredMetadataManager metadata;

    private final LoadingCache<Entry, AsyncFuture<FindTags>> findTags = CacheBuilder.newBuilder().maximumSize(10000)
            .expireAfterWrite(30, TimeUnit.MINUTES).build(new CacheLoader<Entry, AsyncFuture<FindTags>>() {
                @Override
                public AsyncFuture<FindTags> load(Entry e) {
                    return metadata.findTags(e.getGroup(), e.getFilter());
                }
            });

    public AsyncFuture<FindTags> findTags(final String group, final RangeFilter filter) throws ExecutionException {
        final Entry e = new Entry(group, filter);

        return findTags.get(e).on(new FutureDone<FindTags>() {
            @Override
            public void cancelled() {
                findTags.invalidate(e);
            }

            @Override
            public void failed(Throwable e) {
                findTags.invalidate(e);
            }

            @Override
            public void resolved(FindTags result) {
            }
        });
    }

    private final LoadingCache<Entry, AsyncFuture<FindKeys>> findKeys = CacheBuilder.newBuilder().maximumSize(10000)
            .expireAfterWrite(30, TimeUnit.MINUTES).build(new CacheLoader<Entry, AsyncFuture<FindKeys>>() {
                @Override
                public AsyncFuture<FindKeys> load(Entry e) {
                    return metadata.findKeys(e.getGroup(), e.getFilter());
                }
            });

    public AsyncFuture<FindKeys> findKeys(final String group, final RangeFilter filter) throws ExecutionException {
        final Entry e = new Entry(group, filter);

        return findKeys.get(e).on(new FutureDone<FindKeys>() {
            @Override
            public void cancelled() {
                findKeys.invalidate(e);
            }

            @Override
            public void failed(Throwable e) {
                findKeys.invalidate(e);
            }

            @Override
            public void resolved(FindKeys result) {
            }
        });
    }

    @Data
    private static final class Entry {
        final String group;
        final RangeFilter filter;
    }
}
