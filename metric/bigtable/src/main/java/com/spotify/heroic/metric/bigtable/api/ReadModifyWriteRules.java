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

package com.spotify.heroic.metric.bigtable.api;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.bigtable.com.google.protobuf.ByteString;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
public class ReadModifyWriteRules {
    private final List<com.google.bigtable.v2.ReadModifyWriteRule> rules;

    /**
     * Get the list of rules.
     * <p>
     * Package private since it should only be access in the
     * {@link com.spotify.heroic.metric.bigtable.api}
     * package.
     *
     * @return The list of rules.
     */
    List<com.google.bigtable.v2.ReadModifyWriteRule> getRules() {
        return rules;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Data
    public static class Builder {
        final List<com.google.bigtable.v2.ReadModifyWriteRule> rules = new ArrayList<>();

        public Builder increment(
            final String family, final ByteString column, final long value
        ) {
            rules.add(com.google.bigtable.v2.ReadModifyWriteRule
                .newBuilder()
                .setFamilyName(family)
                .setColumnQualifier(column)
                .setIncrementAmount(value)
                .build());

            return this;
        }

        public ReadModifyWriteRules build() {
            return new ReadModifyWriteRules(ImmutableList.copyOf(rules));
        }
    }
}
