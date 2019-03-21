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

package com.spotify.heroic.test;

import java.util.Optional;
import java.util.function.Supplier;

public class TestProperties {
    private final String prefix;

    @java.beans.ConstructorProperties({ "prefix" })
    private TestProperties(final String prefix) {
        this.prefix = prefix;
    }

    public Optional<String> getOptionalString(final String key) {
        final String property = property(key);
        return get(property);
    }

    public String getRequiredString(final String key) {
        final String property = property(key);
        return get(property).orElseThrow(requiredProperty(property));
    }

    private Supplier<IllegalStateException> requiredProperty(final String property) {
        return () -> new IllegalStateException(property + ": required system property missing");
    }

    private Optional<String> get(final String property) {
        return Optional.ofNullable(System.getProperty(property));
    }

    private String property(final String key) {
        return prefix + "." + key;
    }

    public static TestProperties ofPrefix(final String prefix) {
        return new TestProperties(prefix);
    }
}
