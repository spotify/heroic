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

package com.spotify.heroic;

import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.statistics.QueryReporter;
import com.spotify.heroic.statistics.HeroicReporter;
import dagger.Module;
import dagger.Provides;
import javax.inject.Named;
import lombok.Data;

@Module
@Data
public class QueryModule {
    private final OptionalLimit groupLimit;
    private final long smallQueryThreshold;

    @Provides
    @QueryScope
    @Named("groupLimit")
    public OptionalLimit groupLimit() {
        return groupLimit;
    }

    @Provides
    @QueryScope
    @Named("smallQueryThreshold")
    public long smallQueryThreshold() {
        return smallQueryThreshold;
    }

    @Provides
    @QueryScope
    public QueryReporter queryReporter(HeroicReporter heroicReporter) {
        return heroicReporter.newQueryReporter();
    }
}
