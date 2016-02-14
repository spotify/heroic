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

import lombok.Data;

import java.util.Optional;

public interface Validation {
    /**
     * Check that the given field is non-null.
     *
     * @param object The field value to check.
     * @param <T> The type of the field
     * @return The object, if non null.
     * @throws com.spotify.heroic.common.Validation.BodyError if field is null.
     */
    static <T> T bodyNotNull(T object) {
        if (object == null) {
            throw new BodyError("body must be present");
        }

        return object;
    }

    /**
     * Check that the given field is present.
     *
     * @param object The field value to check.
     * @param name The name of the field.
     * @param <T> The type of the field
     * @return The object, if present.
     * @throws com.spotify.heroic.common.Validation.FieldError if field is not present.
     */
    static <T> T fieldIsPresent(Optional<T> object, String name) {
        if (!object.isPresent()) {
            throw new FieldError(name, "must be present");
        }

        return object.get();
    }

    @Data
    class BodyError extends RuntimeException {
        public BodyError(final String message) {
            super(message);
        }
    }

    @Data
    class FieldError extends RuntimeException {
        private final String name;

        public FieldError(final String name, final String message) {
            super(message);
            this.name = name;
        }
    }
}
