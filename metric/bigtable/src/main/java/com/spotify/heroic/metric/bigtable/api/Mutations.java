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
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class Mutations {
    private final List<com.google.bigtable.v2.Mutation> mutations;

    /**
     * Get the list of mutations.
     * <p>
     * Package private since it should only be access in the
     * {@link com.spotify.heroic.metric.bigtable.api}
     * package.
     *
     * @return The list of mutations.
     */
    List<com.google.bigtable.v2.Mutation> getMutations() {
        return mutations;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Data
    public static class Builder {
        final List<com.google.bigtable.v2.Mutation> mutations = new ArrayList<>();

        public Builder setCell(
            String family, ByteString columnQualifier, ByteString value
        ) {
            final com.google.bigtable.v2.Mutation.SetCell.Builder setCell =
                com.google.bigtable.v2.Mutation.SetCell
                    .newBuilder()
                    .setFamilyName(family)
                    .setColumnQualifier(columnQualifier)
                    .setValue(value);

            mutations.add(com.google.bigtable.v2.Mutation.newBuilder().setSetCell(setCell).build());
            return this;
        }

        public Mutations build() {
            return new Mutations(ImmutableList.copyOf(mutations));
        }

        public int size() {
            return mutations.size();
        }
    }
}
