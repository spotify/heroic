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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.grammar.QueryParser;
import lombok.Data;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

@Data
public class ExtraParameters {
    public static final ParameterSpecification CONFIGURE =
        ParameterSpecification.parameter("configure", "Automatically configure all backends.");

    public static final Joiner SCOPE_JOINER = Joiner.on('.');

    private final List<String> scope;
    private final Multimap<String, String> parameters;

    public boolean containsAny(String... keys) {
        return Arrays.stream(keys).anyMatch(parameters::containsKey);
    }

    public Optional<Filter> getFilter(String key, QueryParser parser) {
        final Collection<String> values = parameters.get(key);

        if (values.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(parser.parseFilter(values.iterator().next()));
    }

    public Optional<Integer> getInteger(String key) {
        try {
            return get(key).map(Integer::parseInt);
        } catch (final NumberFormatException e) {
            throw new IllegalStateException(
                "Key " + key + " exists, but does not contain a valid numeric value", e);
        }
    }

    public Optional<String> get(String key) {
        return getEntry(key).stream().findFirst();
    }

    public boolean contains(String key) {
        return parameters.containsKey(key);
    }

    public String require(final String key) {
        return get(key).orElseThrow(
            () -> new IllegalStateException(key + ": is a required parameter"));
    }

    public boolean contains(ParameterSpecification parameter) {
        return contains(parameter.getName());
    }

    public List<String> getAsList(final String key) {
        return ImmutableList.copyOf(getEntry(key));
    }

    public Optional<Boolean> getBoolean(final String key) {
        return get(key).map(s -> "true".equals(s) || "yes".equals(s));
    }

    public Optional<Duration> getDuration(String key) {
        try {
            return get(key).map(Duration::parseDuration);
        } catch (final Exception e) {
            throw new IllegalStateException(
                "Key " + key + " exists, but does not contain a valid duration value", e);
        }
    }

    public ExtraParameters scope(final String scope) {
        return new ExtraParameters(
            ImmutableList.<String>builder().addAll(this.scope).add(scope).build(), parameters);
    }

    private Collection<String> getEntry(String key) {
        return Optional.ofNullable(parameters.get(entryKey(key))).orElseGet(ImmutableList::of);
    }

    private String entryKey(final String key) {
        if (scope.isEmpty()) {
            return key;
        }

        return SCOPE_JOINER.join(ImmutableList.<String>builder().addAll(scope).add(key).build());
    }

    public static ExtraParameters empty() {
        return new ExtraParameters(ImmutableList.of(), ImmutableMultimap.of());
    }

    public static ExtraParameters ofList(final List<String> input) {
        final ImmutableMultimap.Builder<String, String> result = ImmutableMultimap.builder();

        for (final String entry : input) {
            final int index = entry.indexOf('=');

            if (index < 0) {
                result.put(entry, "");
            } else {
                result.put(entry.substring(0, index), entry.substring(index + 1));
            }
        }

        return new ExtraParameters(ImmutableList.of(), result.build());
    }
}
