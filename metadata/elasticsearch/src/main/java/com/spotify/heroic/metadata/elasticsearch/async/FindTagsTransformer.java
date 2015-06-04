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

package com.spotify.heroic.metadata.elasticsearch.async;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import lombok.RequiredArgsConstructor;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.FilterBuilder;

import com.spotify.heroic.elasticsearch.ElasticsearchUtils;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterModifier;
import com.spotify.heroic.metadata.model.FindTagKeys;
import com.spotify.heroic.metadata.model.FindTags;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.LazyTransform;

@RequiredArgsConstructor
public class FindTagsTransformer implements LazyTransform<FindTagKeys, FindTags> {
    private final AsyncFramework async;
    private final ExecutorService executor;
    private final FilterModifier modifier;
    private final Filter filter;
    private final Callable<SearchRequestBuilder> setup;
    private final ElasticsearchUtils.FilterContext ctx;

    @Override
    public AsyncFuture<FindTags> transform(FindTagKeys result) throws Exception {
        final List<AsyncFuture<FindTags>> callbacks = new ArrayList<>();

        for (final String tag : result.getKeys())
            callbacks.add(findSingle(tag));

        return async.collect(callbacks, FindTags.reduce());
    }

    /**
     * Finds a single set of tags, excluding any criteria for this specific set of tags.
     */
    private AsyncFuture<FindTags> findSingle(final String tag) {
        final Filter filter = modifier.removeTag(this.filter, tag);

        final FilterBuilder f = ctx.filter(filter);

        if (f == null)
            return async.resolved(FindTags.EMPTY);

        return async.call(new FindTagsResolver(setup, ctx, f, tag), executor);
    }
}