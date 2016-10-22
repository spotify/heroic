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
import com.google.common.base.Stopwatch;

/**
 * A type that encapsulated behaviour if something is tracing or not.
 * <p>
 * Is serialized as JSON true when enabled or false when disabled.
 */
public interface Tracing {
    Tracing ENABLED = new TracingEnabled();
    Tracing DISABLED = new TracingDisabled();

    /**
     * Check if tracing is enabled or not.
     *
     * @return {@code true} if tracing is enabled, {@code false} otherwise
     */
    @JsonValue
    boolean isEnabled();

    /**
     * Convert a boolean to a tracing instance.
     *
     * @param value Boolean that will be converted
     * @return {@link #enabled()} when value is {@code true}, {@link #disabled()} when value is
     * {@code false}
     */
    @JsonCreator
    static Tracing fromBoolean(final boolean value) {
        return value ? enabled() : disabled();
    }

    /**
     * Get an enabled tracing.
     *
     * @return an enabled tracing
     */
    static Tracing enabled() {
        return ENABLED;
    }

    /**
     * Get a disabled tracing.
     *
     * @return a disabled tracing
     */
    static Tracing disabled() {
        return DISABLED;
    }

    /**
     * Create a new watch.
     *
     * @return a {@link com.spotify.heroic.metric.QueryTrace.Watch}
     */
    QueryTrace.Watch watch();

    /**
     * Create a new watch.
     *
     * @return a {@link com.spotify.heroic.metric.QueryTrace.Watch}
     */
    QueryTrace.NamedWatch watch(final QueryTrace.Identifier what);

    /**
     * Type that implements behaviour when tracing is enabled.
     */
    class TracingEnabled implements Tracing {
        private TracingEnabled() {
        }

        @Override
        public boolean isEnabled() {
            return true;
        }

        @Override
        public QueryTrace.Watch watch() {
            return new QueryTrace.ActiveWatch(Stopwatch.createStarted());
        }

        @Override
        public QueryTrace.NamedWatch watch(final QueryTrace.Identifier what) {
            return new QueryTrace.ActiveNamedWatch(what, Stopwatch.createStarted());
        }
    }

    /**
     * Type that implements behaviour when tracing is disabled.
     */
    class TracingDisabled implements Tracing {
        private TracingDisabled() {
        }

        @Override
        public boolean isEnabled() {
            return false;
        }

        @Override
        public QueryTrace.Watch watch() {
            return QueryTrace.PASSIVE_WATCH;
        }

        @Override
        public QueryTrace.NamedWatch watch(final QueryTrace.Identifier what) {
            return QueryTrace.PASSIVE_NAMED_WATCH;
        }
    }
}
