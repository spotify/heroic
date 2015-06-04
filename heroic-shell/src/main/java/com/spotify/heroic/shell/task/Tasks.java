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

package com.spotify.heroic.shell.task;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;

import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.RangeFilter;

public final class Tasks {
    public static Filter setupFilter(FilterFactory filters, QueryParser parser, QueryParams params) {
        final List<String> query = params.getQuery();

        if (query.isEmpty())
            return filters.t();

        return parser.parseFilter(StringUtils.join(query, " "));
    }

    public abstract static class QueryParamsBase implements QueryParams {
        private final DateRange defaultDateRange;

        public QueryParamsBase() {
            final long now = System.currentTimeMillis();
            final long start = now - TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS);
            this.defaultDateRange = new DateRange(start, now);
        }

        @Override
        public DateRange getRange() {
            return defaultDateRange;
        }
    }

    public static interface QueryParams {
        public List<String> getQuery();

        public DateRange getRange();

        public int getLimit();
    }

    public static RangeFilter setupRangeFilter(FilterFactory filters, QueryParser parser, QueryParams params) {
        final Filter filter = setupFilter(filters, parser, params);
        return new RangeFilter(filter, params.getRange(), params.getLimit());
    }
}
