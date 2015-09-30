/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.http.metadata;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.common.RangeFilter;
import com.spotify.heroic.metadata.FindKeys;
import com.spotify.heroic.metadata.FindTags;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureDone;
import lombok.Data;

public class MetadataResourceCache {
    @Inject
    private ClusterManager cluster;

    private final LoadingCache<Entry, AsyncFuture<FindTags>> findTags = CacheBuilder.newBuilder().maximumSize(10000)
            .expireAfterWrite(30, TimeUnit.MINUTES).build(new CacheLoader<Entry, AsyncFuture<FindTags>>() {
                @Override
                public AsyncFuture<FindTags> load(Entry e) {
                    return cluster.useGroup(e.getGroup()).findTags(e.getFilter());
                }
            });

    public AsyncFuture<FindTags> findTags(final String group, final RangeFilter filter) throws ExecutionException {
        final Entry e = new Entry(group, filter);

        return findTags.get(e).onDone(new FutureDone<FindTags>() {
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
                    return cluster.useGroup(e.getGroup()).findKeys(e.getFilter());
                }
            });

    public AsyncFuture<FindKeys> findKeys(final String group, final RangeFilter filter) throws ExecutionException {
        final Entry e = new Entry(group, filter);

        return findKeys.get(e).onDone(new FutureDone<FindKeys>() {
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
