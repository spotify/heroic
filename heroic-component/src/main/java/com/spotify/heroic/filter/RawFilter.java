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

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.spotify.heroic.ObjectHasher;
import com.spotify.heroic.common.Series;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(of = {"OPERATOR", "filter"}, doNotUseGetters = true)
@JsonTypeName("raw")
public class RawFilter implements Filter {
    public static final String OPERATOR = "q";

    private final String filter;

    @Override
    public boolean apply(Series series) {
        throw new RuntimeException("Not supported");
    }

    @Override
    public <T> T visit(final Visitor<T> visitor) {
        return visitor.visitRaw(this);
    }

    @Override
    public String toString() {
        return "[" + OPERATOR + ", " + filter + "]";
    }

    @Override
    public RawFilter optimize() {
        return this;
    }

    @Override
    public String operator() {
        return OPERATOR;
    }

    @Override
    public int compareTo(Filter o) {
        if (!RawFilter.class.isAssignableFrom(o.getClass())) {
            return operator().compareTo(o.operator());
        }

        final RawFilter other = (RawFilter) o;
        return filter.compareTo(other.getFilter());
    }

    @Override
    public String toDSL() {
        throw new RuntimeException("raw filter cannot be converted to DSL");
    }

    @Override
    public void hashTo(final ObjectHasher hasher) {
        hasher.putObject(getClass(), () -> {
            hasher.putField("filter", filter, hasher.string());
        });
    }
}
