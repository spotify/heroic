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

package com.spotify.heroic.cluster;

import static com.google.common.base.Preconditions.checkNotNull;
import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.RangeFilter;
import com.spotify.heroic.filter.Filter;

@Data
public class GroupFilter {
    private final String group;
    private final RangeFilter filter;

    @JsonCreator
    public GroupFilter(@JsonProperty("group") String group, @JsonProperty("filter") RangeFilter filter) {
        this.group = checkNotNull(group);
        this.filter = checkNotNull(filter);
    }

    public static GroupFilter filterFor(String group, Filter filter, DateRange range) {
        return new GroupFilter(group, new RangeFilter(filter, range, Integer.MAX_VALUE));
    }

    public static GroupFilter filterFor(String group, Filter filter, DateRange range, int limit) {
        return new GroupFilter(group, new RangeFilter(filter, range, limit));
    }
}