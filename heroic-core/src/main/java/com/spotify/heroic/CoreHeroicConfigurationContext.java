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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.dagger.CoreComponent;
import org.eclipse.jetty.util.ConcurrentArrayQueue;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.function.Function;

public class CoreHeroicConfigurationContext implements HeroicConfigurationContext {
    private final ObjectMapper mapper;

    private final ConcurrentArrayQueue<Function<CoreComponent, List<Object>>> resources =
        new ConcurrentArrayQueue<>();

    private final Object lock = new Object();

    @Inject
    public CoreHeroicConfigurationContext(
        @Named("application/heroic-config") final ObjectMapper mapper
    ) {
        this.mapper = mapper;
    }

    @Override
    public void registerType(String name, Class<?> type) {
        synchronized (lock) {
            mapper.registerSubtypes(new NamedType(type, name));
        }
    }

    @Override
    public void resources(Function<CoreComponent, List<Object>> type) {
        resources.add(type);
    }

    @Override
    public List<Function<CoreComponent, List<Object>>> getResources() {
        return ImmutableList.copyOf(resources);
    }
}
