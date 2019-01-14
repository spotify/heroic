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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CollapseInstance extends GroupingAggregation {
    public static final Map<String, String> NO_GROUP = ImmutableMap.of();

    @JsonCreator
    public CollapseInstance(
        @JsonProperty("of") Optional<List<String>> of,
        @JsonProperty("each") AggregationInstance each
    ) {
        super(of, each);
    }

    @Override
    protected Map<String, String> key(final Map<String, String> tags) {
        return getOf().map(of -> {
            // group by 'everything'
            if (of.isEmpty()) {
                return tags;
            }

            final Map<String, String> key = new HashMap<>(tags);

            for (final String o : of) {
                key.remove(o);
            }

            return key;
        }).orElse(NO_GROUP);
    }

    @Override
    protected AggregationInstance newInstance(
        final Optional<List<String>> of, final AggregationInstance each
    ) {
        return new CollapseInstance(of, each);
    }
}
