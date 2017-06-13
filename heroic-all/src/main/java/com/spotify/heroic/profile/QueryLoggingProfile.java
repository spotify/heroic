/*
 * Copyright (c) 2017 Spotify AB.
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

import static com.spotify.heroic.ParameterSpecification.parameter;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.ExtraParameters;
import com.spotify.heroic.HeroicConfig;
import com.spotify.heroic.ParameterSpecification;
import com.spotify.heroic.querylogging.Slf4jQueryLoggingModule;
import java.util.List;
import java.util.Optional;

public class QueryLoggingProfile extends HeroicProfileBase {
    @Override
    public HeroicConfig.Builder build(final ExtraParameters params) throws Exception {
        final HeroicConfig.Builder builder = HeroicConfig.builder();

        final String level = params.get("level").orElse("TRACE");

        // @formatter:off
        return builder
            .queryLogging(
                new Slf4jQueryLoggingModule(Optional.of("query_log"), Optional.of(level))
            );
        // @formatter:on
    }

    @Override
    public Optional<String> scope() {
        return Optional.of("query-logging");
    }

    @Override
    public String description() {
        // @formatter:off
        return "Configures a basic query logger";
        // @formatter:on
    }

    @Override
    public List<ParameterSpecification> options() {
        // @formatter:off
        return ImmutableList.of(
            parameter("level", "Specifies log level", "<level>")
        );
        // @formatter:on
    }
}
