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


import com.spotify.heroic.common.OptionalLimit;

/**
 * A simple class to centralize logic around limiting the number of suggestions requested from ES,
 * from Heroic. It defaults to 10 but any request to the backend (e.g. MemoryBackend) can override
 * that number.
 */
public class NumSuggestionsLimit {

    /**
     * How many suggestions we should request from ES.
     *
     * <p>This applies to the requests made for keys, tag and tag values. This defaults to 10,
     * otherwise * 10,000 is used as the default which is wasteful and could lag the grafana UI.
     */
    public static final int DEFAULT_NUM_SUGGESTIONS_LIMIT = 10;
    private int limit = DEFAULT_NUM_SUGGESTIONS_LIMIT;

    public NumSuggestionsLimit() {
    }

    public NumSuggestionsLimit(int limit) {
        this.limit = limit;
    }

    public int getLimit() {
        return limit;
    }

    /**
     * use this.limit unless limit is not empty, then return the numeric result.
     *
     * @param limit the limit to respect if non-empty, usually from a request object
     * @return `this` - for fluent coding support
     */
    public NumSuggestionsLimit updateLimit(OptionalLimit limit) {
        var num = limit.orElse(OptionalLimit.of(this.limit));
        this.limit = num.asInteger().get();
        return this;
    }

    /**
     * use this.limit unless limit is not empty, then return the numeric result.
     *
     * @param limit the limit to respect if non-empty, usually from a request object
     * @return the resulting, updated numeric limit
     */
    public int updateAndGetLimit(OptionalLimit limit) {
        return updateLimit(limit).getLimit();
    }

    public OptionalLimit asOptionalLimit() {
        return OptionalLimit.of(limit);
    }
}
