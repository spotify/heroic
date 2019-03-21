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

package com.spotify.heroic.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.auto.value.AutoValue;
import com.spotify.heroic.ObjectHasher;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.grammar.DSL;
import java.util.regex.Pattern;

@AutoValue
@JsonTypeName("regex")
public abstract class RegexFilter implements Filter {
    @JsonCreator
    public static RegexFilter create(
        @JsonProperty("tag") String tag, @JsonProperty("value") String value
    ) {
        return new AutoValue_RegexFilter(tag, value);
    }
    public static final String OPERATOR = "~";
    abstract String tag();
    abstract String value();

    @Override
    public boolean apply(Series series) {
        final String tagValue = series.getTags().get(tag());
        return tagValue != null && Pattern.compile(value()).matcher(tagValue).matches();
    }

    @Override
    public <T> T visit(final Visitor<T> visitor) {
        return visitor.visitRegex(this);
    }

    @Override
    public String toString() {
        return "[" + OPERATOR + ", " + tag() + ", " + value() + "]";
    }

    @Override
    public RegexFilter optimize() {
        return this;
    }

    @Override
    public String operator() {
        return OPERATOR;
    }

    @Override
    public int compareTo(Filter o) {
        if (!RegexFilter.class.isAssignableFrom(o.getClass())) {
            return operator().compareTo(o.operator());
        }

        final RegexFilter other = (RegexFilter) o;
        final int first = tag().compareTo(other.tag());

        if (first != 0) {
            return first;
        }

        return value().compareTo(other.value());
    }

    @Override
    public String toDSL() {
        return DSL.dumpString(tag()) + " ~ " + DSL.dumpString(value());
    }

    @Override
    public void hashTo(final ObjectHasher hasher) {
        hasher.putObject(getClass(), () -> {
            hasher.putField("tag", tag(), hasher.string());
            hasher.putField("value", value(), hasher.string());
        });
    }
}
