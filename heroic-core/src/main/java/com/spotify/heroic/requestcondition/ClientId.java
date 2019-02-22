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

package com.spotify.heroic.requestcondition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.spotify.heroic.querylogging.QueryContext;

/**
 * A request condition that match a given client id.
 */
@AutoValue
public abstract class ClientId implements RequestCondition {
    @JsonCreator
    public static ClientId create(@JsonProperty("clientId") String clientId) {
        return new AutoValue_ClientId(clientId);
    }

    abstract String clientId();

    /**
     * Match the HttpContext and optionally provide a feature set to apply to a request.
     */
    @Override
    public boolean matches(final QueryContext context) {
        return context
            .httpContext()
            .flatMap(httpContext -> httpContext.getClientId().map(clientId()::equals))
            .orElse(false);
    }
}
