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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.cluster.ClusterShard;
import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Indicates that a specific shard of the request failed and information on which and why.
 *
 * @author udoprog
 */
@Data
public class ShardError implements RequestError {
    private final List<String> nodes;
    private final Map<String, String> shard;
    private final String error;

    @JsonCreator
    public static ShardError create(
        @JsonProperty("nodes") List<String> nodes, @JsonProperty("shard") Map<String, String> shard,
        @JsonProperty("error") String error
    ) {
        return new ShardError(nodes, shard, error);
    }

    public static ShardError fromThrowable(ClusterShard c, Throwable e) {
        final List<String> nodes =
            c.getGroups().stream().map(Object::toString).collect(Collectors.toList());
        final String message = errorMessage(e);
        return new ShardError(nodes, c.getShard(), message);
    }

    private static String errorMessage(Throwable e) {
        final String message = e.getMessage() == null ? "<null>" : e.getMessage();

        if (e.getCause() == null) {
            return message;
        }

        return String.format("%s, caused by %s", message, errorMessage(e.getCause()));
    }
}
