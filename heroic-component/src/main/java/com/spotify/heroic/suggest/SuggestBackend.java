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

import com.spotify.heroic.common.Collected;
import com.spotify.heroic.common.Grouped;
import com.spotify.heroic.common.Initializing;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import eu.toolchain.async.AsyncFuture;
import io.opencensus.trace.Span;
import java.util.SortedSet;

public interface SuggestBackend extends Grouped, Initializing, Collected {

    /**
     * Configure the suggest backend.
     *
     * This will assert that all required settings and mappings exists and are configured
     * correctly for the given backend.
     */
    AsyncFuture<Void> configure();

    /**
     * Return a set of suggestions for the most relevant tag values (given the number of tags
     * available).
     */
    AsyncFuture<TagValuesSuggest> tagValuesSuggest(TagValuesSuggest.Request request);

    /**
     * Return an estimated count of the given tags.
     */
    AsyncFuture<TagKeyCount> tagKeyCount(TagKeyCount.Request request);

    AsyncFuture<TagSuggest> tagSuggest(TagSuggest.Request request);

    AsyncFuture<KeySuggest> keySuggest(KeySuggest.Request request);

    AsyncFuture<TagValueSuggest> tagValueSuggest(TagValueSuggest.Request request);

    AsyncFuture<WriteSuggest> write(WriteSuggest.Request request);

    default AsyncFuture<WriteSuggest> write(WriteSuggest.Request request, Span parentSpan) {
        // Ignore the parent span if the module does not specifically implement it.
        return write(request);
    }

    default Statistics getStatistics() {
        return Statistics.empty();
    }
}
