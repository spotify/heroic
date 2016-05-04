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

import com.spotify.heroic.filter.Filter.MatchKey;
import com.spotify.heroic.filter.Filter.Regex;
import com.spotify.heroic.filter.impl.AndFilterImpl;
import com.spotify.heroic.filter.impl.FalseFilterImpl;
import com.spotify.heroic.filter.impl.HasTagFilterImpl;
import com.spotify.heroic.filter.impl.MatchKeyFilterImpl;
import com.spotify.heroic.filter.impl.MatchTagFilterImpl;
import com.spotify.heroic.filter.impl.NotFilterImpl;
import com.spotify.heroic.filter.impl.OrFilterImpl;
import com.spotify.heroic.filter.impl.RegexFilterImpl;
import com.spotify.heroic.filter.impl.StartsWithFilterImpl;
import com.spotify.heroic.filter.impl.TrueFilterImpl;

import java.util.Arrays;
import java.util.List;

public final class CoreFilterFactory implements FilterFactory {
    @Override
    public Filter.And and(Filter... filters) {
        return new AndFilterImpl(Arrays.asList(filters));
    }

    @Override
    public Filter.And and(List<Filter> filters) {
        return new AndFilterImpl(filters);
    }

    @Override
    public Filter.Or or(Filter... filters) {
        return new OrFilterImpl(Arrays.asList(filters));
    }

    @Override
    public Filter.Or or(List<Filter> filters) {
        return new OrFilterImpl(filters);
    }

    @Override
    public Filter.Not not(Filter filter) {
        return new NotFilterImpl(filter);
    }

    @Override
    public Filter.True t() {
        return TrueFilterImpl.get();
    }

    @Override
    public Filter.False f() {
        return FalseFilterImpl.get();
    }

    @Override
    public Filter.MatchTag matchTag(String key, String value) {
        return new MatchTagFilterImpl(key, value);
    }

    @Override
    public Filter.HasTag hasTag(String tag) {
        return new HasTagFilterImpl(tag);
    }

    @Override
    public Filter.StartsWith startsWith(String tag, String value) {
        return new StartsWithFilterImpl(tag, value);
    }

    @Override
    public MatchKey matchKey(String value) {
        return new MatchKeyFilterImpl(value);
    }

    @Override
    public Regex regex(String tag, String value) {
        return new RegexFilterImpl(tag, value);
    }
}
