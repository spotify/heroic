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

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Inject;
import com.spotify.heroic.filter.impl.HasTagFilterImpl;
import com.spotify.heroic.filter.impl.MatchTagFilterImpl;
import com.spotify.heroic.filter.impl.OrFilterImpl;

public final class CoreFilterModifier implements FilterModifier {
    @Inject
    private FilterFactory filters;

    @Override
    public Filter removeTag(Filter filter, String tag) {
        if (filter instanceof Filter.And) {
            final Filter.And and = (Filter.And) filter;

            final List<Filter> statements = new ArrayList<Filter>();

            for (final Filter f : and.terms()) {
                statements.add(removeTag(f, tag));
            }

            if (statements.isEmpty()) {
                return filters.t();
            }

            return filters.and(statements).optimize();
        }

        if (filter instanceof OrFilterImpl) {
            final OrFilterImpl or = (OrFilterImpl) filter;

            final List<Filter> statements = new ArrayList<Filter>();

            for (final Filter f : or.getStatements()) {
                statements.add(removeTag(f, tag));
            }

            if (statements.isEmpty()) {
                return filters.t();
            }

            return new OrFilterImpl(statements).optimize();
        }

        if (filter instanceof MatchTagFilterImpl) {
            final MatchTagFilterImpl matchTag = (MatchTagFilterImpl) filter;

            if (matchTag.getTag().equals(tag)) {
                return filters.t();
            }

            return matchTag;
        }

        if (filter instanceof HasTagFilterImpl) {
            final HasTagFilterImpl hasTag = (HasTagFilterImpl) filter;

            if (hasTag.getTag().equals(tag)) {
                return filters.t();
            }

            return hasTag;
        }

        return filter;
    }
}
