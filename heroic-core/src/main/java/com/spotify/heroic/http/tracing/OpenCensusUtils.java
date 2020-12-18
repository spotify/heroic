/*
 * Copyright (c) 2018 Spotify AB.
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

package com.spotify.heroic.http.tracing;

import io.opencensus.trace.Status;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

class OpenCensusUtils {
    private OpenCensusUtils() { }

    static String formatList(List<?> list) {
        return list.stream().map(Object::toString).collect(Collectors.joining(", "));
    }

    static String formatProviders(Iterable<?> providers) {
        return StreamSupport.stream(providers.spliterator(), false)
            .map((provider) -> provider.getClass().getName())
            .collect(Collectors.joining(", "));
    }

    @SuppressWarnings("LineLength")
    static Status mapStatusCode(int status) {
        // Mapping from:
        // https://github.com/census-instrumentation/opencensus-specs/blob/master/trace/HTTP.md#mapping-from-http-status-codes-to-trace-status-codes
        final Status traceStatus;
        if (status < 200) {
            traceStatus = Status.UNKNOWN;
        } else if (status < 400) {
            traceStatus = Status.OK;
        } else if (status == 400) {
            traceStatus = Status.INVALID_ARGUMENT;
        } else if (status == 404) {
            traceStatus = Status.NOT_FOUND;
        } else if (status == 403) {
            traceStatus = Status.PERMISSION_DENIED;
        } else if (status == 401) {
            traceStatus = Status.UNAUTHENTICATED;
        } else if (status == 429) {
            traceStatus = Status.RESOURCE_EXHAUSTED;
        } else if (status == 501) {
            traceStatus = Status.UNIMPLEMENTED;
        } else if (status == 503) {
            traceStatus = Status.UNAVAILABLE;
        } else if (status == 504) {
            traceStatus = Status.DEADLINE_EXCEEDED;
        } else if (status < 600) {
            traceStatus = Status.INTERNAL;
        } else {
            traceStatus = Status.UNKNOWN;
        }

        return traceStatus;
    }
}
