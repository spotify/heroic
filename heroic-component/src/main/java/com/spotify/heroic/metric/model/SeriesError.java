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

package com.spotify.heroic.metric.model;

import java.util.List;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.exceptions.UserException;

/**
 * Indicates that a specific shard of the request failed and information on which and why.
 *
 * @author udoprog
 */
@Data
public class SeriesError implements RequestError {
    private final List<TagValues> tags;
    private final String error;
    private final boolean internal;

    @JsonCreator
    public static SeriesError create(@JsonProperty("tags") List<TagValues> tags, @JsonProperty("error") String error,
            @JsonProperty("internal") Boolean internal) {
        return new SeriesError(tags, error, internal);
    }

    public static SeriesError fromThrowable(final List<TagValues> shard, Throwable e) {
        final String message = errorMessage(e);
        final boolean internal = !(e instanceof UserException);
        return new SeriesError(shard, message, internal);
    }

    private static String errorMessage(Throwable e) {
        final String message = e.getMessage() == null ? "<null>" : e.getMessage();

        if (e.getCause() == null)
            return message;

        return String.format("%s, caused by %s", message, errorMessage(e.getCause()));
    }
}