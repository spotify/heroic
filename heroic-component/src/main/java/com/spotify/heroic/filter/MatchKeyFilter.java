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

@AutoValue
@JsonTypeName("key")
public abstract class MatchKeyFilter implements Filter {
    @JsonCreator
    public static MatchKeyFilter create(@JsonProperty("key") String key) {
        return new AutoValue_MatchKeyFilter(key);
    }

    public static final String OPERATOR = "key";
    public abstract String key();

    @Override
    public boolean apply(Series series) {
        return series.getKey().equals(key());
    }

    @Override
    public <T> T visit(final Visitor<T> visitor) {
        return visitor.visitMatchKey(this);
    }

    @Override
    public String toString() {
        return "[" + OPERATOR + ", " + key() + "]";
    }

    @Override
    public MatchKeyFilter optimize() {
        return this;
    }

    @Override
    public String operator() {
        return OPERATOR;
    }

    @Override
    public int compareTo(Filter o) {
        if (!MatchKeyFilter.class.isAssignableFrom(o.getClass())) {
            return operator().compareTo(o.operator());
        }

        final MatchKeyFilter other = (MatchKeyFilter) o;
        return key().compareTo(other.key());
    }

    @Override
    public String toDSL() {
        return "$key = " + DSL.dumpString(key());
    }

    @Override
    public void hashTo(final ObjectHasher hasher) {
        hasher.putObject(getClass(), () -> {
            hasher.putField("key", key(), hasher.string());
        });
    }
}
