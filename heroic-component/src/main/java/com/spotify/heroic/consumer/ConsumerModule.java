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

package com.spotify.heroic.consumer;

import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.ingestion.IngestionComponent;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.statistics.ConsumerReporter;
import lombok.Data;

import java.util.Optional;

public interface ConsumerModule {
    Out module(PrimaryComponent primary, IngestionComponent ingestion, In in, String id);

    Optional<String> id();

    String buildId(int i);

    interface Builder {
        ConsumerModule build();
    }

    @Data
    public static class In {
        private final ConsumerReporter consumerReporter;
    }

    public interface Out {
        Consumer consumer();

        default LifeCycle consumerLife() {
            return LifeCycle.empty();
        }
    }
}
