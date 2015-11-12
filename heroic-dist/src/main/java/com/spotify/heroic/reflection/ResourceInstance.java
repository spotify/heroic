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

package com.spotify.heroic.reflection;

import eu.toolchain.async.Transform;
import lombok.RequiredArgsConstructor;

/**
 * Wraps an instance of something that have been reflectively created from a single line of a
 * resource file.
 *
 * The restricted access to the instance asserts that any thrown exception is wrapped with the
 * context that declares the instance, giving the user information of where the resource that caused
 * the error came from (e.g. which line from a particular file).
 */
@RequiredArgsConstructor
public class ResourceInstance<T> {
    private final ResourceLineContext ctx;
    private final T instance;

    public <R> R invoke(Transform<T, R> transform) throws ResourceException {
        try {
            return transform.transform(instance);
        } catch (Exception e) {
            throw ctx.exception(e.getMessage(), e);
        }
    }
}
