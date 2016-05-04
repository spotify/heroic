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

package com.spotify.heroic.generator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.common.Series;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class RandomMetadataGenerator implements MetadataGenerator {
    private static final List<String> ROLES = ImmutableList.of("database", "web", "ingestor");

    private static final List<String> WHATS = ImmutableList.of("disk-used", "teleported-goats");

    private final Random random = new Random();

    @JsonCreator
    public RandomMetadataGenerator() {
    }

    @Override
    public List<Series> generate(int count) {
        final ImmutableList.Builder<Series> series = ImmutableList.builder();

        for (int i = 0; i < count; i++) {
            series.add(generateOne(i));
        }

        return series.build();
    }

    private Series generateOne(int index) {
        final String key = generateKey(index);
        final Map<String, String> tags = generateTags(index);
        return Series.of(key, tags);
    }

    private String generateKey(int index) {
        return String.format("key-%d", index);
    }

    private Map<String, String> generateTags(int index) {
        final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

        builder.put("host", String.format("%d.example.com", index));
        builder.put("role", randomPick(ROLES));
        builder.put("what", randomPick(WHATS));

        return builder.build();
    }

    private String randomPick(final List<String> pool) {
        return pool.get(random.nextInt(pool.size()));
    }

    public static RandomMetadataGenerator buildDefault() {
        return new RandomMetadataGenerator();
    }
}
