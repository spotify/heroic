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

package com.spotify.heroic.rpc.httprpc.model;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.model.DateRange;

@Data
public final class RpcTagValuesSuggest {
    private final String group;
    private final Filter filter;
    private final Integer limit;
    private final DateRange range;

    @JsonCreator
    public static RpcTagValuesSuggest create(@JsonProperty("filter") Filter filter, @JsonProperty("range") DateRange range,
            @JsonProperty("group") String group, @JsonProperty("limit") Integer limit) {
        return new RpcTagValuesSuggest(group, filter, limit, range);
    }
}