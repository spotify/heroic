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
     * Check that the given field is present.
     *
     * @param object The field value to check.
     * @param name The name of the field.
     * @param <T> The type of the field
     * @return The object, if present.
     * @throws com.spotify.heroic.common.Validation.MissingField if field is not present.
     */
    static <T> T fieldIsPresent(Optional<T> object, String name) {
        if (!object.isPresent()) {
            throw new MissingField(name, "must be present");
        }

        return object.get();
    }

    @Data
    class MissingBody extends RuntimeException {
        public MissingBody(final String message) {
            super(message);
        }
    }

    @Data
    class MissingField extends RuntimeException {
        private final String name;

        public MissingField(final String name, final String message) {
            super(message);
            this.name = name;
        }
    }
}
