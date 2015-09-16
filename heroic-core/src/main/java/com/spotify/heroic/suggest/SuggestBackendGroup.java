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

import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.RangeFilter;
import com.spotify.heroic.common.SelectedGroup;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.WriteResult;
import com.spotify.heroic.statistics.LocalMetadataManagerReporter;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.Data;
import lombok.ToString;

@Data
@ToString(of = { "backends" })
public class SuggestBackendGroup implements SuggestBackend {
    private final AsyncFramework async;
    private final SelectedGroup<SuggestBackend> backends;
    private final LocalMetadataManagerReporter reporter;

    @Override
    public AsyncFuture<Void> configure() {
        final List<AsyncFuture<Void>> callbacks = new ArrayList<>();

        run(new InternalOperation() {
            @Override
            public void run(int disabled, SuggestBackend backend) {
                callbacks.add(backend.configure());
            }
        });

        return async.collectAndDiscard(callbacks);
    }

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

        return async.collect(callbacks, TagValuesSuggest.reduce(filter.getLimit(), groupLimit)).on(
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

        return async.collect(callbacks, TagValueSuggest.reduce(filter.getLimit())).on(
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

        return async.collect(callbacks, TagKeyCount.reduce(filter.getLimit())).on(reporter.reportTagKeySuggest());
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

        return async.collect(callbacks, TagSuggest.reduce(filter.getLimit())).on(reporter.reportTagSuggest());
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

        return async.collect(callbacks, KeySuggest.reduce(filter.getLimit())).on(reporter.reportKeySuggest());
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
    public Groups getGroups() {
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