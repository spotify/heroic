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

package com.spotify.heroic.dagger;

import com.spotify.heroic.HeroicConfig;
import com.spotify.heroic.common.ServiceInfo;
import dagger.Module;
import dagger.Provides;
import lombok.RequiredArgsConstructor;

import javax.inject.Named;
import java.util.Optional;
import java.util.function.Supplier;

@RequiredArgsConstructor
@Module
public class EarlyModule {
    private final HeroicConfig config;
    private final Optional<String> id;

    private volatile boolean stopping = false;

    @Provides
    @EarlyScope
    ServiceInfo service() {
        return new ServiceInfo(config.getService(), config.getVersion(), id.orElse("heroic"));
    }

    @Provides
    @EarlyScope
    HeroicConfig config() {
        return config;
    }

    @Provides
    @Named("stopping")
    @EarlyScope
    Supplier<Boolean> stopping() {
        return () -> stopping;
    }

    @Provides
    @Named("stopSignal")
    @EarlyScope
    Runnable stopSignal() {
        return () -> stopping = true;
    }
}
