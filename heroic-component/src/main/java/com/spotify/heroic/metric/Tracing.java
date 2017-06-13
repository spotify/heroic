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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Stopwatch;

/**
 * A type that encapsulated behaviour if something is tracing or not.
 * <p>
 * Is serialized as JSON true when enabled or false when disabled.
 */
public enum Tracing {
    NONE, DEFAULT, DETAILED;

    /**
     * Test if tracing is enabled at a DEFAULT level.
     *
     * @return {@code true} if tracing is enabled
     */
    public boolean isEnabled() {
        return isEnabled(DEFAULT);
    }

    /**
     * Check if tracing is enabled or not.
     *
     * @return {@code true} if tracing is enabled
     */
    public boolean isEnabled(final Tracing query) {
        return query.ordinal() <= this.ordinal();
    }

    /**
     * Create a new watch for the DEFAULT tracing level.
     *
     * @param what what to watch
     * @return a {@link com.spotify.heroic.metric.QueryTrace.NamedWatch}
     */
    public QueryTrace.NamedWatch watch(final QueryTrace.Identifier what) {
        return watch(what, DEFAULT);
    }

    /**
     * Create a new watch.
     *
     * @return a {@link com.spotify.heroic.metric.QueryTrace.NamedWatch}
     */
    public QueryTrace.NamedWatch watch(final QueryTrace.Identifier what, final Tracing query) {
        if (isEnabled(query)) {
            return new QueryTrace.ActiveNamedWatch(what, Stopwatch.createStarted());
        }

        return QueryTrace.PASSIVE_NAMED_WATCH;
    }

    /**
     * Create tracing from a json node.
     *
     * @param node node to create from
     * @return tracing corresponding to the node
     */
    @JsonCreator
    static Tracing fromJson(JsonNode node) {
        switch (node.getNodeType()) {
            case STRING:
                return Tracing.valueOf(node.asText().toUpperCase());
            case BOOLEAN:
                return fromBoolean(node.asBoolean());
            default:
                throw new IllegalArgumentException(
                    "expected string or boolean, got: " + node.getNodeType());
        }
    }

    /**
     * Convert to json.
     *
     * @return json representation (that will be serialized)
     */
    @JsonValue
    String toJson() {
        return toString().toLowerCase();
    }

    /**
     * Convert a boolean to a tracing instance.
     *
     * @param value Boolean that will be converted
     * @return {@code ENABLED} when value is {@code true}, {@code NONE} when value is {@code
     * false}
     */
    public static Tracing fromBoolean(final boolean value) {
        return value ? DEFAULT : NONE;
    }
}
