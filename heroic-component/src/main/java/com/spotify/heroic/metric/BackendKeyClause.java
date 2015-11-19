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

import java.util.List;

import com.google.common.collect.ImmutableList;

import lombok.Data;

/**
 * Generic filtering clauses for BackendKey's.
 *
 * These should be converted by the receiving backend to native types of filters.
 *
 * @author udoprog
 */
public interface BackendKeyClause {
    /**
     * Match all keys.
     *
     * @return A {@link BackendKeyClause} matching all keys.
     */
    public static All all() {
        return new All();
    }

    /**
     * Match all keys, if all given sub-clauses match.
     *
     * @param clauses Sub-clauses to match.
     * @return A {@link BackendKeyClause} matching if all sub-clauses match.
     */
    public static And and(final Iterable<BackendKeyClause> clauses) {
        return new And(ImmutableList.copyOf(clauses));
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

    /**
     * Limit the number of matching keys returned.
     *
     * This must only be applied once, else the innermost clause wins.
     *
     * @param clause The clause to limit.
     * @param limit The limit to apply.
     * @return A {@link BackendKeyClause} limiting the number of matching keys.
     */
    public static BackendKeyClause limited(BackendKeyClause clause, int limit) {
        return new Limited(clause, limit);
    }

    @Data
    class All implements BackendKeyClause {
    }

    @Data
    class Limited implements BackendKeyClause {
        private final BackendKeyClause clause;
        private final int limit;
    }

    @Data
    class GTE implements BackendKeyClause {
        private final BackendKey key;
    }

    @Data
    class LT implements BackendKeyClause {
        private final BackendKey key;
    }

    @Data
    class GTEPercentage implements BackendKeyClause {
        private final float percentage;
    }

    @Data
    class LTPercentage implements BackendKeyClause {
        private final float percentage;
    }

    @Data
    class GTEToken implements BackendKeyClause {
        private final long token;
    }

    @Data
    class LTToken implements BackendKeyClause {
        private final long token;
    }

    @Data
    class And implements BackendKeyClause {
        private final List<BackendKeyClause> clauses;
    }
}
