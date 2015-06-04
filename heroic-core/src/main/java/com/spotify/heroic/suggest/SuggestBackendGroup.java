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

package com.spotify.heroic.suggest;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import lombok.Data;
import lombok.ToString;

import com.spotify.heroic.metric.model.WriteResult;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.RangeFilter;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.statistics.LocalMetadataManagerReporter;
import com.spotify.heroic.suggest.model.KeySuggest;
import com.spotify.heroic.suggest.model.MatchOptions;
import com.spotify.heroic.suggest.model.TagKeyCount;
import com.spotify.heroic.suggest.model.TagSuggest;
import com.spotify.heroic.suggest.model.TagValueSuggest;
import com.spotify.heroic.suggest.model.TagValuesSuggest;
import com.spotify.heroic.utils.SelectedGroup;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

@Data
@ToString(of = { "backends" })
public class SuggestBackendGroup implements SuggestBackend {
    private final AsyncFramework async;
    private final SelectedGroup<SuggestBackend> backends;
    private final LocalMetadataManagerReporter reporter;

    @Override
    public AsyncFuture<TagValuesSuggest> tagValuesSuggest(final RangeFilter filter, final List<String> exclude,
            final int groupLimit) {
        final List<AsyncFuture<TagValuesSuggest>> callbacks = new ArrayList<>();

        run(new InternalOperation() {
            @Override
            public void run(int disabled, SuggestBackend backend) {
                callbacks.add(backend.tagValuesSuggest(filter, exclude, groupLimit));
            }
        });

        return async.collect(callbacks, TagValuesSuggest.reduce(filter.getLimit(), groupLimit)).onAny(
                reporter.reportTagValuesSuggest());
    }

    @Override
    public AsyncFuture<TagValueSuggest> tagValueSuggest(final RangeFilter filter, final String key) {
        final List<AsyncFuture<TagValueSuggest>> callbacks = new ArrayList<>();

        run(new InternalOperation() {
            @Override
            public void run(int disabled, SuggestBackend backend) {
                callbacks.add(backend.tagValueSuggest(filter, key));
            }
        });

        return async.collect(callbacks, TagValueSuggest.reduce(filter.getLimit())).onAny(
                reporter.reportTagValueSuggest());
    }

    @Override
    public AsyncFuture<TagKeyCount> tagKeyCount(final RangeFilter filter) {
        final List<AsyncFuture<TagKeyCount>> callbacks = new ArrayList<>();

        run(new InternalOperation() {
            @Override
            public void run(int disabled, SuggestBackend backend) {
                callbacks.add(backend.tagKeyCount(filter));
            }
        });

        return async.collect(callbacks, TagKeyCount.reduce(filter.getLimit())).onAny(reporter.reportTagKeySuggest());
    }

    @Override
    public AsyncFuture<TagSuggest> tagSuggest(final RangeFilter filter, final MatchOptions options, final String key,
            final String value) {
        final List<AsyncFuture<TagSuggest>> callbacks = new ArrayList<>();

        run(new InternalOperation() {
            @Override
            public void run(int disabled, SuggestBackend backend) {
                callbacks.add(backend.tagSuggest(filter, options, key, value));
            }
        });

        return async.collect(callbacks, TagSuggest.reduce(filter.getLimit())).onAny(reporter.reportTagSuggest());
    }

    @Override
    public AsyncFuture<KeySuggest> keySuggest(final RangeFilter filter, final MatchOptions options, final String key) {
        final List<AsyncFuture<KeySuggest>> callbacks = new ArrayList<>();

        run(new InternalOperation() {
            @Override
            public void run(int disabled, SuggestBackend backend) {
                callbacks.add(backend.keySuggest(filter, options, key));
            }
        });

        return async.collect(callbacks, KeySuggest.reduce(filter.getLimit())).onAny(reporter.reportKeySuggest());
    }

    @Override
    public AsyncFuture<WriteResult> write(final Series series, final DateRange range) {
        final List<AsyncFuture<WriteResult>> callbacks = new ArrayList<>();

        run(new InternalOperation() {
            @Override
            public void run(int disabled, SuggestBackend backend) {
                callbacks.add(backend.write(series, range));
            }
        });

        return async.collect(callbacks, WriteResult.merger());
    }

    @Override
    public boolean isReady() {
        boolean ready = true;

        for (final SuggestBackend backend : backends) {
            ready = ready && backend.isReady();
        }

        return ready;
    }

    @Override
    public Set<String> getGroups() {
        return backends.groups();
    }

    private void run(InternalOperation op) {
        for (final SuggestBackend b : backends) {
            op.run(backends.getDisabled(), b);
        }
    }

    public static interface InternalOperation {
        void run(int disabled, SuggestBackend backend);
    }
}