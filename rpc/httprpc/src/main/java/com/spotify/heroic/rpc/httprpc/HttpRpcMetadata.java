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

package com.spotify.heroic.rpc.httprpc;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.cluster.model.NodeCapability;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class HttpRpcMetadata {
    public static final int DEFAULT_VERSION = 0;

    public static final Set<NodeCapability> DEFAULT_CAPABILITIES = new HashSet<>();
    static {
        DEFAULT_CAPABILITIES.add(NodeCapability.QUERY);
    }

    private final int version;
    private final UUID id;
    private final Map<String, String> tags;
    private final Set<NodeCapability> capabilities;

    @JsonCreator
    public static HttpRpcMetadata create(@JsonProperty("version") Integer version, @JsonProperty("id") UUID id,
            @JsonProperty(value = "tags", required = false) Map<String, String> tags,
            @JsonProperty(value = "capabilities", required = false) Set<NodeCapability> capabilities) {
        if (version == null)
            version = DEFAULT_VERSION;

        if (capabilities == null)
            capabilities = DEFAULT_CAPABILITIES;

        if (id == null)
            throw new IllegalArgumentException("'id' must be specified");

        return new HttpRpcMetadata(version, id, tags, capabilities);
    }
}
