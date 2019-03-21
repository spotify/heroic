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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.HeroicContext;
import com.spotify.heroic.HeroicCoreInstance;
import com.spotify.heroic.ShellTasks;
import com.spotify.heroic.common.Features;
import com.spotify.heroic.conditionalfeatures.ConditionalFeatures;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.lifecycle.LifeCycleManager;
import com.spotify.heroic.statistics.HeroicReporter;

import java.util.Optional;
import javax.inject.Named;

public interface PrimaryComponent extends EarlyComponent {
    HeroicCoreInstance instance();

    HeroicReporter reporter();

    @Named("features")
    Features features();

    @Named("application/json+internal")
    ObjectMapper internalMapper();

    @Named("application/json")
    ObjectMapper jsonMapper();

    QueryParser queryParser();

    ShellTasks tasks();

    LifeCycleManager lifeCycleManager();

    HeroicContext context();

    Optional<ConditionalFeatures> conditionalFeatures();
}
