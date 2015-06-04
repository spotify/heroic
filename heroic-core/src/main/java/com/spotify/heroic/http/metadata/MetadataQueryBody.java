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

package com.spotify.heroic.http.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.filter.impl.MatchKeyFilterImpl;
import com.spotify.heroic.filter.impl.TrueFilterImpl;
import com.spotify.heroic.http.query.QueryDateRange;
import com.spotify.heroic.model.DateRange;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class MetadataQueryBody {
    private static final QueryDateRange DEFAULT_DATE_RANGE = new QueryDateRange.Relative(TimeUnit.DAYS, 7);
    private static final int DEFAULT_LIMIT = 50;

    /**
     * Only include time series which match the exact key.
     */
    private final String matchKey;

    /**
     * Only include time series which matches the exact key/value combination.
     */
    private final Map<String, String> matchTags;

    /**
     * Only include time series which has the following tags.
     */
    private final Set<String> hasTags;

    /**
     * A general set of filters. If this is combined with the other mechanisms, all the filters will be AND:ed together.
     */
    private final Filter filter;

    /**
     * The date range to query for.
     */
    private final DateRange range;

    private final int limit;

    public Filter makeFilter(FilterFactory filters) {
        final List<Filter> statements = new ArrayList<>();

        if (filter != null) {
            statements.add(filter);
        }

        if (matchTags != null && !matchTags.isEmpty()) {
            for (final Map.Entry<String, String> entry : matchTags.entrySet()) {
                statements.add(filters.matchTag(entry.getKey(), entry.getValue()));
            }
        }

        if (hasTags != null && !hasTags.isEmpty()) {
            for (final String tag : hasTags) {
                statements.add(filters.hasTag(tag));
            }
        }

        if (matchKey != null)
            statements.add(new MatchKeyFilterImpl(matchKey));

        if (statements.size() == 0)
            return TrueFilterImpl.get();

        if (statements.size() == 1)
            return statements.get(0).optimize();

        return filters.and(statements).optimize();
    }

    @JsonCreator
    public static MetadataQueryBody create(@JsonProperty("matchKey") String matchKey,
            @JsonProperty("matchTags") Map<String, String> matchTags, @JsonProperty("hasTags") Set<String> hasTags,
            @JsonProperty("filter") Filter filter, @JsonProperty("range") QueryDateRange range,
            @JsonProperty("limit") Integer limit) {
        if (range == null)
            range = DEFAULT_DATE_RANGE;

        if (limit == null)
            limit = DEFAULT_LIMIT;

        return new MetadataQueryBody(matchKey, matchTags, hasTags, filter, range.buildDateRange(), limit);
    }

    public static MetadataQueryBody create() {
        return create(null, null, null, null, null, null);
    }
}
