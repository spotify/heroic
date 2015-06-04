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

import java.util.List;

public interface FilterFactory {
    public Filter.And and(Filter... filters);

    public Filter.And and(List<Filter> filters);

    public Filter.Or or(Filter... filters);

    public Filter.Or or(List<Filter> filters);

    public Filter.Not not(Filter filter);

    public Filter.True t();

    public Filter.False f();

    public Filter.MatchKey matchKey(String value);

    public Filter.MatchTag matchTag(String key, String value);

    public Filter.HasTag hasTag(String tag);

    public Filter.StartsWith startsWith(String tag, String value);

    public Filter.Regex regex(String tag, String value);
}
