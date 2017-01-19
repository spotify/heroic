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

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

public final class CoreFilterModifier implements FilterModifier {
    @Inject
    public CoreFilterModifier() {
    }

    @Override
    public Filter removeTag(Filter filter, String tag) {
        return filter.visit(new Filter.Visitor<Filter>() {
            @Override
            public Filter visitAnd(final AndFilter and) {
                final List<Filter> statements = new ArrayList<Filter>();

                for (final Filter f : and.terms()) {
                    statements.add(removeTag(f, tag));
                }

                if (statements.isEmpty()) {
                    return TrueFilter.get();
                }

                return new AndFilter(statements).optimize();
            }

            @Override
            public Filter visitOr(final OrFilter or) {
                final List<Filter> statements = new ArrayList<Filter>();

                for (final Filter f : or.getStatements()) {
                    statements.add(removeTag(f, tag));
                }

                if (statements.isEmpty()) {
                    return TrueFilter.get();
                }

                return new OrFilter(statements).optimize();
            }

            @Override
            public Filter visitMatchTag(final MatchTagFilter matchTag) {
                if (matchTag.getTag().equals(tag)) {
                    return TrueFilter.get();
                }

                return matchTag;
            }

            @Override
            public Filter visitHasTag(final HasTagFilter hasTag) {
                if (hasTag.getTag().equals(tag)) {
                    return TrueFilter.get();
                }

                return hasTag;
            }

            @Override
            public Filter defaultAction(final Filter filter) {
                return filter;
            }
        });
    }
}
