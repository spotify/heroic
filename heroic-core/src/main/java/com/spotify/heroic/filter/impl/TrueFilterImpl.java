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

import lombok.Data;
import lombok.EqualsAndHashCode;

import com.spotify.heroic.filter.Filter;

@Data
@EqualsAndHashCode(of = { "OPERATOR" }, doNotUseGetters = true)
public class TrueFilterImpl implements Filter.True {
    public static final String OPERATOR = "true";

    private static final Filter.True instance = new TrueFilterImpl();

    public static Filter.True get() {
        return instance;
    }

    @Override
    public String toString() {
        return "[" + OPERATOR + "]";
    }

    @Override
    public TrueFilterImpl optimize() {
        return this;
    }

    @Override
    public String operator() {
        return OPERATOR;
    }

    @Override
    public Filter invert() {
        return FalseFilterImpl.get();
    }

    @Override
    public int compareTo(Filter o) {
        if (!Filter.True.class.isAssignableFrom(o.getClass()))
            return operator().compareTo(o.operator());

        return 0;
    }
}