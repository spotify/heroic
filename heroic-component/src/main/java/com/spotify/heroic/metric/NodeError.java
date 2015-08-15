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

import java.util.Map;
import java.util.UUID;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.NodeMetadata;

/**
 * Indicates that a specific shard of the request failed and information on which and why.
 *
 * @author udoprog
 */
@Data
public class NodeError implements RequestError {
    private final UUID nodeId;
    private final String node;
    private final Map<String, String> tags;
    private final String error;
    private final boolean internal;

    @JsonCreator
    public static NodeError create(@JsonProperty("nodeId") UUID nodeId, @JsonProperty("nodeUri") String node,
            @JsonProperty("tags") Map<String, String> tags, @JsonProperty("error") String error,
            @JsonProperty("internal") Boolean internal) {
        return new NodeError(nodeId, node, tags, error, internal);
    }

    public static NodeError fromThrowable(ClusterNode c, Throwable e) {
        final NodeMetadata m = c.metadata();
        final String message = errorMessage(e);
        final boolean internal = true;
        return new NodeError(m.getId(), c.toString(), m.getTags(), message, internal);
    }

    public static NodeError fromThrowable(final UUID nodeId, final String node, final Map<String, String> shard,
            Throwable e) {
        final String message = errorMessage(e);
        final boolean internal = true;
        return new NodeError(nodeId, node, shard, message, internal);
    }

    private static String errorMessage(Throwable e) {
        final String message = e.getMessage() == null ? "<null>" : e.getMessage();

        if (e.getCause() == null)
            return message;

        return String.format("%s, caused by %s", message, errorMessage(e.getCause()));
    }
}
