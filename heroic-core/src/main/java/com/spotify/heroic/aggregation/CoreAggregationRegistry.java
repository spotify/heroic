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

package com.spotify.heroic.aggregation;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import eu.toolchain.serializer.Serializer;
import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.Map;

/**
 * Serializes aggregation configurations.
 * <p>
 * Each aggregation configuration is packed into a Composite which has the type of the aggregation
 * as a prefixed short.
 *
 * @author udoprog
 */
@RequiredArgsConstructor
public class CoreAggregationRegistry implements AggregationRegistry {
    final Serializer<String> string;

    final Map<Class<? extends Aggregation>, String> definitionMap = new HashMap<>();
    final Map<Class<? extends AggregationInstance>, String> instanceMap = new HashMap<>();
    final Map<String, Serializer<? extends AggregationInstance>> serializerMap = new HashMap<>();
    final Map<String, AggregationDSL> builderMap = new HashMap<>();

    private final Object lock = new Object();

    @Override
    public <A extends Aggregation, I extends AggregationInstance> void register(
        final String id, final Class<A> type, final Class<I> instanceType,
        final Serializer<I> instanceSerializer, final AggregationDSL dsl
    ) {
        synchronized (lock) {
            if (serializerMap.containsKey(id)) {
                throw new IllegalArgumentException(
                    "An aggregation with the same id (" + id + ") is already registered");
            }

            if (definitionMap.containsKey(type)) {
                throw new IllegalArgumentException(
                    "An aggregation with the same type (" + type.getCanonicalName() +
                        ") is already registered");
            }

            if (instanceMap.containsKey(instanceType)) {
                throw new IllegalArgumentException("An aggregation instance with the same type (" +
                    instanceType.getCanonicalName() + ") is already registered");
            }

            definitionMap.put(type, id);
            instanceMap.put(instanceType, id);
            builderMap.put(id, dsl);
            serializerMap.put(id, instanceSerializer);
        }
    }

    public Module module() {
        final SimpleModule m = new SimpleModule("aggregationRegistry");

        for (final Map.Entry<Class<? extends AggregationInstance>, String> e : instanceMap
            .entrySet()) {
            m.registerSubtypes(new NamedType(e.getKey(), e.getValue()));
        }

        for (final Map.Entry<Class<? extends Aggregation>, String> e : definitionMap.entrySet()) {
            m.registerSubtypes(new NamedType(e.getKey(), e.getValue()));
        }

        return m;
    }

    @Override
    public AggregationFactory newAggregationFactory() {
        return new CoreAggregationFactory(builderMap);
    }

    @Override
    public AggregationSerializer newAggregationSerializer() {
        return new CoreAggregationSerializer(string, instanceMap, serializerMap);
    }
}
