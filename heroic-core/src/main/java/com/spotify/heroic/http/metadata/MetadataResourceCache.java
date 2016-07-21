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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.spotify.heroic.QueryManager;
import com.spotify.heroic.metadata.FindKeys;
import com.spotify.heroic.metadata.FindTags;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureDone;
import lombok.Data;

import javax.inject.Inject;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class MetadataResourceCache {
    private final QueryManager query;

    @Inject
    public MetadataResourceCache(final QueryManager query) {
        this.query = query;
    }

    private final LoadingCache<Grouped<FindTags.Request>, AsyncFuture<FindTags>> findTags =
        CacheBuilder
            .newBuilder()
            .maximumSize(10000)
            .expireAfterWrite(30, TimeUnit.MINUTES)
            .build(new CacheLoader<Grouped<FindTags.Request>, AsyncFuture<FindTags>>() {
                @Override
                public AsyncFuture<FindTags> load(Grouped<FindTags.Request> g) {
                    return query.useOptionalGroup(g.getGroup()).findTags(g.getValue());
                }
            });

    public AsyncFuture<FindTags> findTags(
        final Optional<String> group, final FindTags.Request request
    ) throws ExecutionException {
        final Grouped<FindTags.Request> key = new Grouped<>(group, request);

        return findTags.get(key).onDone(new FutureDone<FindTags>() {
            @Override
            public void cancelled() {
                findTags.invalidate(key);
            }

            @Override
            public void failed(Throwable e) {
                findTags.invalidate(key);
            }

            @Override
            public void resolved(FindTags result) {
            }
        });
    }

    private final LoadingCache<Grouped<FindKeys.Request>, AsyncFuture<FindKeys>> findKeys =
        CacheBuilder
            .newBuilder()
            .maximumSize(10000)
            .expireAfterWrite(30, TimeUnit.MINUTES)
            .build(new CacheLoader<Grouped<FindKeys.Request>, AsyncFuture<FindKeys>>() {
                @Override
                public AsyncFuture<FindKeys> load(Grouped<FindKeys.Request> g) {
                    return query.useOptionalGroup(g.getGroup()).findKeys(g.getValue());
                }
            });

    public AsyncFuture<FindKeys> findKeys(
        final Optional<String> group, final FindKeys.Request request
    ) throws ExecutionException {
        final Grouped<FindKeys.Request> key = new Grouped<>(group, request);

        return findKeys.get(key).onDone(new FutureDone<FindKeys>() {
            @Override
            public void cancelled() {
                findKeys.invalidate(key);
            }

            @Override
            public void failed(Throwable e) {
                findKeys.invalidate(key);
            }

            @Override
            public void resolved(FindKeys result) {
            }
        });
    }

    @Data
    static class Grouped<T> {
        private final Optional<String> group;
        private final T value;
    }
}
