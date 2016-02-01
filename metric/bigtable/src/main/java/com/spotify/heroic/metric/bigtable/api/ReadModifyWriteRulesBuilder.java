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

import com.google.bigtable.v1.ReadModifyWriteRule;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;

@Data
public class ReadModifyWriteRulesBuilder {
    final List<ReadModifyWriteRule> rules = new ArrayList<>();

    public ReadModifyWriteRulesBuilder increment(final String family, final ByteString column,
            final long value) {
        rules.add(ReadModifyWriteRule.newBuilder().setFamilyName(family).setColumnQualifier(column)
                .setIncrementAmount(value).build());
        return this;
    }

    public ReadModifyWriteRules build() {
        return new ReadModifyWriteRules(ImmutableList.copyOf(rules));
    }

    public int size() {
        return rules.size();
    }
}
