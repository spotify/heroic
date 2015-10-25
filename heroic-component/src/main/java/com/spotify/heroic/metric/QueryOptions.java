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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = CoreQueryOptions.class, name = "core") })
public interface QueryOptions {
    /**
     * Indicates if tracing is enabled.
     *
     * Traces queries will include a {@link QueryTrace} object that indicates detailed timings of the query.
     *
     * @return {@code true} if tracing is enabled.
     */
    public boolean isTracing();

    public static QueryOptions defaults() {
        return CoreQueryOptions.DEFAULTS;
    }

    static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private boolean tracing = false;

        public Builder tracing(boolean tracing) {
            this.tracing = tracing;
            return this;
        }

        public QueryOptions build() {
            return new CoreQueryOptions(tracing);
        }
    }
}