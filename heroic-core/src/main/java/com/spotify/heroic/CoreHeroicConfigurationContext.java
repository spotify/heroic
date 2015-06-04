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

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;

public class CoreHeroicConfigurationContext implements HeroicConfigurationContext {
    @Inject
    @Named("application/heroic-config")
    private ObjectMapper mapper;

    private final List<Class<?>> resources = new ArrayList<>();
    private final List<Module> modules = new ArrayList<>();

    @Override
    public void registerType(String name, Class<?> type) {
        mapper.registerSubtypes(new NamedType(type, name));
    }

    @Override
    public void resource(Class<?> type) {
        resources.add(type);
    }

    @Override
    public void module(Module module) {
        modules.add(module);
    }

    @Override
    public List<Class<?>> getResources() {
        return ImmutableList.copyOf(resources);
    }

    @Override
    public List<Module> getModules() {
        return ImmutableList.copyOf(modules);
    }
}
