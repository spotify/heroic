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
import java.util.Optional;

/**
 * A simple class to centralize logic around limiting the number of suggestions requested from ES by
 * Heroic. It defaults to 50 but any request to the backend (e.g. MemoryBackend) can override that
 * number.
 */
public class NumSuggestionsLimit {

    /** No request is allowed to request more than this many tags, keys or tag values. */
    public static final int LIMIT_CEILING = 250;

    /**
     * How many suggestions we should request from ES, unless the suggest API request specifies
     * otherwise.
     *
     * <p>This applies to the requests made for keys, tag and tag values. This defaults to 50,
     * otherwise * 10,000 is used as the default which is wasteful and could lag the grafana UI.
     */
    public static final int DEFAULT_LIMIT = 50;

    private final int limit;

    private NumSuggestionsLimit() {
        limit = DEFAULT_LIMIT;
    }

    private NumSuggestionsLimit(int limit) {
        int okLimit = Math.min(LIMIT_CEILING, limit);
        this.limit = okLimit;
    }

    public static NumSuggestionsLimit of(Optional<Integer> limit) {
        return limit.isEmpty() ? new NumSuggestionsLimit() : new NumSuggestionsLimit(limit.get());
    }

    public static NumSuggestionsLimit of() {
        return new NumSuggestionsLimit();
    }

    public static NumSuggestionsLimit of(int limit) {
        return new NumSuggestionsLimit(limit);
    }

    public int getLimit() {
        return limit;
    }

    /**
     * use this.limit unless limit is not empty, then return the numeric result.
     *
     * @param limit the limit to respect if non-empty, usually from a request object
     * @return a new NSL object - for fluent coding support
     */
    public NumSuggestionsLimit create(OptionalLimit limit) {
        int num = limit.orElse(OptionalLimit.of(this.limit)).asInteger().get();
        return new NumSuggestionsLimit(num);
    }

    /**
     * use this.limit unless limit is not empty, then return the numeric result.
     *
     * @param limit the limit to respect if non-empty, usually from a request object
     * @return the resulting, updated numeric limit
     */
    public int calculateNewLimit(OptionalLimit limit) {
        return create(limit).getLimit();
    }

    public OptionalLimit asOptionalLimit() {
        return OptionalLimit.of(limit);
    }

    public Optional<Integer> asOptionalInt() {
        return Optional.of(getLimit());
    }
}
