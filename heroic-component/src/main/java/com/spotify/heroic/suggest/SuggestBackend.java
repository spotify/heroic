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

import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Grouped;
import com.spotify.heroic.common.Initializing;
import com.spotify.heroic.common.RangeFilter;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.WriteResult;

import eu.toolchain.async.AsyncFuture;

public interface SuggestBackend extends Grouped, Initializing {
    public AsyncFuture<Void> configure();

    /**
     * Return a set of suggestions for the most relevant tag values (given the number of tags available).
     */
    public AsyncFuture<TagValuesSuggest> tagValuesSuggest(RangeFilter filter, List<String> exclude, int groupLimit);

    /**
     * Return an estimated count of the given tags.
     */
    public AsyncFuture<TagKeyCount> tagKeyCount(RangeFilter filter);

    public AsyncFuture<TagSuggest> tagSuggest(RangeFilter filter, MatchOptions options, String key, String value);

    public AsyncFuture<KeySuggest> keySuggest(RangeFilter filter, MatchOptions options, String key);

    public AsyncFuture<TagValueSuggest> tagValueSuggest(RangeFilter filter, String key);

    public AsyncFuture<WriteResult> write(Series series, DateRange range);
}