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

import lombok.Data;

/**
 * Indicated that an error happened during the query.
 *
 * The only information available is a message.
 */
@Data
public class QueryError implements RequestError {
    private final String error;

    /**
     * Build a query error from a message.
     *
     * @param message Message to build error from
     * @return a {@link com.spotify.heroic.metric.QueryError}
     */
    public static RequestError fromMessage(final String message) {
        return new QueryError(message);
    }

    /**
     * Build a query error from a throwable's message.
     *
     * @param e Throwable to build erroo from.
     * @return a {@link com.spotify.heroic.metric.QueryError}
     */
    public static RequestError fromThrowable(final Throwable e) {
        return new QueryError(e.getMessage());
    }
}
