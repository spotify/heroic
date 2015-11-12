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

import java.util.List;

import com.google.common.collect.ImmutableList;
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
        return async.collectAndDiscard(run(b -> b.configure()));
    }

    @Override
    public AsyncFuture<TagValuesSuggest> tagValuesSuggest(final RangeFilter filter,
            final List<String> exclude, final int groupLimit) {
        return async
                .collect(run(b -> b.tagValuesSuggest(filter, exclude, groupLimit)),
                        TagValuesSuggest.reduce(filter.getLimit(), groupLimit))
                .onDone(reporter.reportTagValuesSuggest());
    }

    @Override
    public AsyncFuture<TagValueSuggest> tagValueSuggest(final RangeFilter filter,
            final String key) {
        return async
                .collect(run(b -> b.tagValueSuggest(filter, key)),
                        TagValueSuggest.reduce(filter.getLimit()))
                .onDone(reporter.reportTagValueSuggest());
    }

    @Override
    public AsyncFuture<TagKeyCount> tagKeyCount(final RangeFilter filter) {
        return async.collect(run(b -> b.tagKeyCount(filter)), TagKeyCount.reduce(filter.getLimit()))
                .onDone(reporter.reportTagKeySuggest());
    }

    @Override
    public AsyncFuture<TagSuggest> tagSuggest(final RangeFilter filter, final MatchOptions options,
            final String key, final String value) {
        return async.collect(run(b -> b.tagSuggest(filter, options, key, value)),
                TagSuggest.reduce(filter.getLimit())).onDone(reporter.reportTagSuggest());
    }

    @Override
    public AsyncFuture<KeySuggest> keySuggest(final RangeFilter filter, final MatchOptions options,
            final String key) {
        return async.collect(run(b -> b.keySuggest(filter, options, key)),
                KeySuggest.reduce(filter.getLimit())).onDone(reporter.reportKeySuggest());
    }

    @Override
    public AsyncFuture<WriteResult> write(final Series series, final DateRange range) {
        return async.collect(run(b -> b.write(series, range)), WriteResult.merger());
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

    @Override
    public boolean isEmpty() {
        return backends.isEmpty();
    }

    @Override
    public int size() {
        return backends.size();
    }

    private <T> List<T> run(InternalOperation<T> op) {
        final ImmutableList.Builder<T> result = ImmutableList.builder();

        for (final SuggestBackend b : backends) {
            result.add(op.run(b));
        }

        return result.build();
    }

    public static interface InternalOperation<T> {
        T run(SuggestBackend backend);
    }
}
