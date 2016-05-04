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

import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Grouped;
import com.spotify.heroic.common.Initializing;
import com.spotify.heroic.common.RangeFilter;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.WriteResult;
import eu.toolchain.async.AsyncFuture;

import java.util.List;
import java.util.Optional;

public interface SuggestBackend extends Grouped, Initializing {
    AsyncFuture<Void> configure();

    /**
     * Return a set of suggestions for the most relevant tag values (given the number of tags
     * available).
     */
    AsyncFuture<TagValuesSuggest> tagValuesSuggest(
        RangeFilter filter, List<String> exclude, int groupLimit
    );

    /**
     * Return an estimated count of the given tags.
     */
    AsyncFuture<TagKeyCount> tagKeyCount(RangeFilter filter);

    AsyncFuture<TagSuggest> tagSuggest(
        RangeFilter filter, MatchOptions options, Optional<String> key, Optional<String> value
    );

    AsyncFuture<KeySuggest> keySuggest(
        RangeFilter filter, MatchOptions options, Optional<String> key
    );

    AsyncFuture<TagValueSuggest> tagValueSuggest(RangeFilter filter, Optional<String> key);

    AsyncFuture<WriteResult> write(Series series, DateRange range);

    default Statistics getStatistics() {
        return Statistics.empty();
    }
}
