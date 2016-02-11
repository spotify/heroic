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

import java.lang.reflect.Constructor;
import java.util.Objects;

public final class ReflectionUtils {
    public static <T> T buildInstance(String className, Class<T> expectedType) {
        Objects.requireNonNull(className, "className");

        final Class<?> clazz;

        try {
            clazz = Class.forName(className);
        } catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException("No such class: " + className, e);
        }

        if (!expectedType.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException(
                "Class is not subtype of: " + expectedType.getCanonicalName());
        }

        @SuppressWarnings("unchecked") final Class<T> target = (Class<T>) clazz;
        return buildInstance(target);
    }

    public static <T> T buildInstance(final Class<T> target) {
        final Constructor<T> constructor;

        try {
            constructor = target.getConstructor();
        } catch (NoSuchMethodException | SecurityException e) {
            throw new IllegalArgumentException(
                "Cannot find empty constructor for class: " + target.getCanonicalName(), e);
        }

        try {
            return constructor.newInstance();
        } catch (IllegalArgumentException | ReflectiveOperationException e) {
            throw new IllegalArgumentException(
                "Failed to create instance of class: " + target.getCanonicalName(), e);
        }
    }
}
