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

import com.spotify.heroic.ObjectHasher;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.grammar.DSL;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(of = {"OPERATOR", "value"}, doNotUseGetters = true)
public class MatchKeyFilter implements Filter {
    public static final String OPERATOR = "key";

    private final String value;

    @Override
    public boolean apply(Series series) {
        return series.getKey().equals(value);
    }

    @Override
    public <T> T visit(final Visitor<T> visitor) {
        return visitor.visitMatchKey(this);
    }

    @Override
    public String toString() {
        return "[" + OPERATOR + ", " + value + "]";
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
        if (!MatchKeyFilter.class.equals(o.getClass())) {
            return operator().compareTo(o.operator());
        }

        final MatchKeyFilter other = (MatchKeyFilter) o;
        return value.compareTo(other.value);
    }

    @Override
    public String toDSL() {
        return "$key = " + DSL.dumpString(value);
    }

    @Override
    public void hashTo(final ObjectHasher hasher) {
        hasher.putObject(getClass(), () -> {
            hasher.putField("value", value, hasher.string());
        });
    }
}
