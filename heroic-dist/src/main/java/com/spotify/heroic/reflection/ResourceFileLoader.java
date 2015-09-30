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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.HeroicService.Configuration;

@RequiredArgsConstructor
public final class ResourceFileLoader {
    public static <T> List<ResourceInstance<T>> loadInstances(String path, ClassLoader loader, Class<T> expected)
            throws ResourceException {
        final List<ResourceInstance<T>> instances = new ArrayList<>();

        final URL resource = loader.getResource(path);

        if (resource == null)
            return instances;

        final ResourcePathContext pathCtx = new ResourcePathContext(resource.toString());

        try (final InputStream stream = resource.openStream()) {
            try (final Reader reader = new InputStreamReader(stream)) {
                try (final BufferedReader buffered = new BufferedReader(reader)) {
                    int lineNumber = 0;

                    while (true) {
                        final String line = buffered.readLine();

                        if (line == null)
                            break;

                        final ResourceLineContext ctx = new ResourceLineContext(pathCtx, ++lineNumber);

                        try {
                            final String trimmed = line.trim();

                            // skip comments
                            if (trimmed.startsWith("#"))
                                continue;

                            final Class<?> c = Class.forName(trimmed, true, loader);

                            if (!expected.isAssignableFrom(c))
                                throw ctx.exception(trimmed + " does not extend "
                                        + Configuration.class.getCanonicalName());

                            @SuppressWarnings("unchecked")
                            final Class<T> type = (Class<T>) c;

                            final Constructor<T> constructor = type.getConstructor();
                            final T newInstance = constructor.newInstance();

                            instances.add(new ResourceInstance<T>(ctx, newInstance));
                        } catch (RuntimeException | ReflectiveOperationException e) {
                            throw ctx.exception(e.toString(), e);
                        }
                    }
                }
            }
        } catch (RuntimeException | IOException e) {
            throw pathCtx.exception("failed to load resource", e);
        }

        return instances;
    }
}