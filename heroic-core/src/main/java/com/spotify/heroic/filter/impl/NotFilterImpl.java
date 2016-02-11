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

package com.spotify.heroic.filter.impl;

import com.spotify.heroic.common.Series;
import com.spotify.heroic.filter.Filter;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(of = {"OPERATOR", "filter"}, doNotUseGetters = true)
public class NotFilterImpl implements Filter.Not {
    public static final String OPERATOR = "not";

    private final Filter filter;

    @Override
    public boolean apply(Series series) {
        return !filter.apply(series);
    }

    @Override
    public String toString() {
        return "[" + OPERATOR + ", " + filter + "]";
    }

    @Override
    public Filter optimize() {
        if (filter instanceof Filter.Not) {
            return ((Filter.Not) filter).first().optimize();
        }

        return new NotFilterImpl(filter.optimize());
    }

    @Override
    public String operator() {
        return OPERATOR;
    }

    @Override
    public Filter first() {
        return filter;
    }

    @Override
    public int compareTo(Filter o) {
        if (!Filter.Not.class.isAssignableFrom(o.getClass())) {
            return operator().compareTo(o.operator());
        }

        return filter.compareTo(o);
    }

    @Override
    public String toDSL() {
        return "!(" + filter.toDSL() + ")";
    }
}
