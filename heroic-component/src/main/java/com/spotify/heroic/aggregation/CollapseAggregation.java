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

package com.spotify.heroic.aggregation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.EqualsAndHashCode;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

@EqualsAndHashCode(of = { "NAME" }, callSuper = true)
public class CollapseAggregation extends GroupingAggregation {
    public static final Map<String, String> NO_GROUP = ImmutableMap.of();
    public static final String NAME = "collapse";

    @JsonCreator
    public CollapseAggregation(@JsonProperty("of") List<String> of, @JsonProperty("each") Aggregation each) {
        super(of, each);
    }

    /**
     * Generate a key for this specific group.
     * 
     * @param tags The tags of a specific group.
     * @return
     */
    protected Map<String, String> key(final Map<String, String> tags) {
        final List<String> of = getOf();

        if (of == null)
            return NO_GROUP;

        // group by 'everything'
        if (of.isEmpty())
            return tags;

        final Map<String, String> key = new HashMap<>(tags);

        for (final String o : of)
            key.remove(o);

        return key;
    }
}