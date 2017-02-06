/*
 * Copyright (c) 2017 Spotify AB.
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

package com.spotify.heroic.querylogging;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Optional;
import java.util.UUID;
import lombok.Data;

@Data
public class QueryContext {
    private final UUID queryId;
    private final Optional<JsonNode> clientContext;
    private final Optional<HttpContext> httpContext;

    public static QueryContext empty() {
        return create(Optional.empty());
    }

    public static QueryContext create(final Optional<JsonNode> clientContext) {
        return new QueryContext(UUID.randomUUID(), clientContext, Optional.empty());
    }

    public static QueryContext create(
        final Optional<JsonNode> clientContext, final HttpContext httpContext
    ) {
        return new QueryContext(UUID.randomUUID(), clientContext, Optional.of(httpContext));
    }
}
