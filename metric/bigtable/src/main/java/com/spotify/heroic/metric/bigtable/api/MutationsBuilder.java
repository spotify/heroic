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

import com.google.bigtable.v1.Mutation;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;

@Data
public class MutationsBuilder {
    final List<Mutation> mutations = new ArrayList<>();

    public MutationsBuilder setCell(String family, ByteString columnQualifier,
            ByteString value) {
        final Mutation.SetCell.Builder setCell = Mutation.SetCell.newBuilder().setFamilyName(family)
                .setColumnQualifier(columnQualifier).setValue(value);

        mutations.add(Mutation.newBuilder().setSetCell(setCell).build());
        return this;
    }

    public Mutations build() {
        return new Mutations(ImmutableList.copyOf(mutations));
    }

    public int size() {
        return mutations.size();
    }
}
