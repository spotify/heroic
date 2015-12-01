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

package com.spotify.heroic.metric;

import java.util.Optional;

import lombok.Data;

@Data
public class BackendKeyFilter {
    private final Optional<Start> start;
    private final Optional<End> end;
    private final Optional<Integer> limit;

    public static BackendKeyFilter of() {
        return new BackendKeyFilter(Optional.empty(), Optional.empty(), Optional.empty());
    }

    public BackendKeyFilter withStart(Start start) {
        return new BackendKeyFilter(Optional.of(start), end, limit);
    }

    public BackendKeyFilter withEnd(End end) {
        return new BackendKeyFilter(start, Optional.of(end), limit);
    }

    public BackendKeyFilter withLimit(int limit) {
        return new BackendKeyFilter(start, end, Optional.of(limit));
    }

    /**
     * Match keys strictly smaller than the given key.
     *
     * @param key Key to match against.
     * @return A {@link BackendKeyClause} matching if a key is strictly smaller than the given key.
     */
    public static LT lt(final BackendKey key) {
        return new LT(key);
    }

    /**
     * Match keys strictly greater than the given key.
     *
     * @param key Key to match against.
     * @return A {@link BackendKeyClause} matching if a key is strictly greater than the given key.
     */
    public static GT gt(final BackendKey key) {
        return new GT(key);
    }

    /**
     * Match keys greater or equal to the given key.
     *
     * @param key The key to match against.
     * @return A {@link BackendKeyClause} matching if a key is greater or equal to the given key.
     */
    public static GTE gte(final BackendKey key) {
        return new GTE(key);
    }

    /**
     * Match keys smaller than the given percentage.
     *
     * @param key The percentage to match against, should be a value between {@code [0, 1]}
     * @return A {@link BackendKeyClause} matching if a key is smaller than a given percentage.
     */
    public static LTPercentage ltPercentage(final float percentage) {
        return new LTPercentage(percentage);
    }

    /**
     * Match keys larger or equal to the given percentage.
     *
     * @param percentage The percentage to match against, should be a value between {@code [0, 1]}.
     * @return A {@link BackendKeyClause} matching if a key is larger or equal to the given
     *         percentage.
     */
    public static GTEPercentage gtePercentage(final float percentage) {
        return new GTEPercentage(percentage);
    }

    /**
     * Match keys smaller than the given token.
     *
     * @param token The token to match against.
     * @return A {@link BackendKeyClause} matching if a key is smaller than the given token.
     */
    public static LTToken ltToken(final long token) {
        return new LTToken(token);
    }

    /**
     * Match keys larger or equal to the given token.
     *
     * @param token The token to match against.
     * @return A {@link BackendKeyClause} matching if a key is larger or equal to the given token.
     */
    public static GTEToken gteToken(final long token) {
        return new GTEToken(token);
    }

    public interface Start {
    }

    public interface End {
    }

    @Data
    public static class GT implements Start {
        private final BackendKey key;
    }

    @Data
    public static class GTE implements Start {
        private final BackendKey key;
    }

    @Data
    public static class GTEPercentage implements Start {
        private final float percentage;
    }

    @Data
    public static class GTEToken implements Start {
        private final long token;
    }

    @Data
    public static class LT implements End {
        private final BackendKey key;
    }

    @Data
    public static class LTPercentage implements End {
        private final float percentage;
    }

    @Data
    public static class LTToken implements End {
        private final long token;
    }
}
