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

package com.spotify.heroic.profile;

import com.spotify.heroic.ExtraParameters;
import com.spotify.heroic.HeroicConfig;
import com.spotify.heroic.cache.memory.MemoryCacheModule;

public class MemoryCacheProfile extends HeroicProfileBase {
    @Override
    public HeroicConfig.Builder build(final ExtraParameters params) throws Exception {
        // @formatter:off
        return HeroicConfig.builder()
            .cache(
                MemoryCacheModule.builder()
            );
        // @formatter:on
    }

    @Override
    public String description() {
        // @formatter:off
        return "Configures in-memory cache";
        // @formatter:on
    }
}
