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

package com.spotify.heroic.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ModuleIdBuilder {
    private final Map<String, Integer> seen = new HashMap<>();

    /**
     * Build a unique identifier for the given object.
     * <p>
     * If the object implements {@link com.spotify.heroic.common.DynamicModuleId}, or is annotated
     * with {@link com.spotify.heroic.common.ModuleId}, use that as the basis for the identifier.
     * Otherwise, use the package in which the Object is contained will be the base identifier.
     *
     * @param object Object to build id for.
     * @return Unique id for the given object.
     */
    public String buildId(Object object) {
        Optional<String> dynamic = Optional.empty();

        if (object instanceof DynamicModuleId) {
            dynamic = ((DynamicModuleId) object).id();
        }

        /* prefer dynamic over @ModuleId annotation */
        final Optional<String> candidate = Optionals.firstPresent(dynamic, Optional
            .ofNullable(object.getClass().getAnnotation(ModuleId.class))
            .map(ModuleId::value));

        /* fall back to using package. */
        final String id = candidate.orElseGet(() -> object.getClass().getPackage().getName());

        final int index = seen.getOrDefault(id, 0);
        seen.put(id, index + 1);

        /* first seen, use the immediate base id */
        if (index == 0) {
            return id;
        }

        /* more than one with the same base id */
        return id + "#" + index;
    }
}
