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

package com.spotify.heroic.elasticsearch.test;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.elasticsearch.ClientSetup;
import com.spotify.heroic.elasticsearch.StandaloneClientSetup;
import com.spotify.heroic.elasticsearch.TransportClientSetup;
import com.spotify.heroic.test.TestProperties;

public class ElasticsearchTestUtils {
    public static ClientSetup clientSetup() {
        final TestProperties properties = TestProperties.ofPrefix("it.elasticsearch");

        return properties.getOptionalString("remote").<ClientSetup>map(v -> {
            final TransportClientSetup.Builder builder = TransportClientSetup.builder();

            properties
                .getOptionalString("clusterName")
                .map(ImmutableList::of)
                .ifPresent(builder::seeds);

            properties.getOptionalString("clusterName").ifPresent(builder::clusterName);

            return builder.build();
        }).orElseGet(() -> StandaloneClientSetup.builder().build());
    }
}
