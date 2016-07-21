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

package com.spotify.heroic.suggest.elasticsearch;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.function.Function;

public class ElasticsearchSuggestUtils {
    public static Map<String, Object> loadJsonResource(
        String path, Function<String, String> replace
    ) {
        final String fullPath =
            ElasticsearchSuggestModule.class.getPackage().getName() + "/" + path;

        try (final InputStream input = ElasticsearchSuggestModule.class
            .getClassLoader()
            .getResourceAsStream(fullPath)) {

            if (input == null) {
                throw new IllegalArgumentException("No such resource: " + fullPath);
            }

            final String content =
                replace.apply(CharStreams.toString(new InputStreamReader(input, Charsets.UTF_8)));

            return JsonXContent.jsonXContent.createParser(content).map();
        } catch (final IOException e) {
            throw new RuntimeException("Failed to load json resource: " + path);
        }
    }

    public static Function<String, String> variables(final Map<String, String> variables) {
        return new StrSubstitutor(variables, "{{", "}}")::replace;
    }
}
