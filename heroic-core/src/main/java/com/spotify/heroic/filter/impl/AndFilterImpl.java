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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.filter.Filter;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

@Data
@EqualsAndHashCode(of = {"OPERATOR", "statements"}, doNotUseGetters = true)
public class AndFilterImpl implements Filter.And {
    public static final String OPERATOR = "and";

    private final List<Filter> statements;

    @Override
    public boolean apply(Series series) {
        return statements.stream().allMatch(s -> s.apply(series));
    }

    @Override
    public String toString() {
        final List<String> parts = new ArrayList<String>(statements.size() + 1);
        parts.add(OPERATOR);

        for (final Filter statement : statements) {
            if (statement == null) {
                parts.add("<null>");
            } else {
                parts.add(statement.toString());
            }
        }

        return "[" + StringUtils.join(parts, ", ") + "]";
    }

    @Override
    public Filter optimize() {
        return optimize(flatten(this.statements));
    }

    private static SortedSet<Filter> flatten(final Collection<Filter> statements) {
        final SortedSet<Filter> result = new TreeSet<>();

        for (final Filter f : statements) {
            final Filter o = f.optimize();

            if (o == null) {
                continue;
            }

            if (o instanceof Filter.And) {
                result.addAll(((Filter.And) o).terms());
                continue;
            }

            if (o instanceof Filter.Not) {
                final Filter.Not not = (Filter.Not) o;

                if (not.first() instanceof Filter.Or) {
                    result.addAll(collapseNotOr((Filter.Or) not.first()));
                    continue;
                }
            }

            result.add(o);
        }

        return result;
    }

    private static List<Filter> collapseNotOr(Filter.Or first) {
        return ImmutableList.copyOf(
            first.terms().stream().map(t -> new NotFilterImpl(t).optimize()).iterator());
    }

    private static Filter optimize(SortedSet<Filter> statements) {
        final SortedSet<Filter> result = new TreeSet<>();

        root:
        for (final Filter f : statements) {
            if (f instanceof Filter.Not) {
                final Filter.Not not = (Filter.Not) f;

                if (statements.contains(not.first())) {
                    return FalseFilterImpl.get();
                }

                result.add(f);
                continue;
            }

            /**
             * If there exists two MatchTag statements, but they check for different values.
             */
            if (f instanceof Filter.MatchTag) {
                final Filter.MatchTag outer = (Filter.MatchTag) f;

                for (final Filter inner : statements) {
                    if (inner.equals(outer)) {
                        continue;
                    }

                    if (inner instanceof Filter.MatchTag) {
                        final Filter.MatchTag matchTag = (Filter.MatchTag) inner;

                        if (!outer.first().equals(matchTag.first())) {
                            continue;
                        }

                        if (!FilterComparatorUtils.isEqual(outer.second(), matchTag.second())) {
                            return FalseFilterImpl.get();
                        }
                    }
                }

                result.add(f);
                continue;
            }

            if (f instanceof Filter.MatchTag) {
                final Filter.MatchTag outer = (Filter.MatchTag) f;

                for (final Filter inner : statements) {
                    if (inner.equals(outer)) {
                        continue;
                    }

                    if (inner instanceof Filter.MatchTag) {
                        final Filter.MatchTag tag = (Filter.MatchTag) inner;

                        if (!outer.first().equals(tag.first())) {
                            continue;
                        }

                        if (!FilterComparatorUtils.isEqual(outer.second(), tag.second())) {
                            return FalseFilterImpl.get();
                        }
                    }
                }

                result.add(f);
                continue;
            }

            // optimize away prefixes which encompass eachother.
            // Example: foo ^ hello and foo ^ helloworld -> foo ^ helloworld
            if (f instanceof Filter.StartsWith) {
                final Filter.StartsWith outer = (Filter.StartsWith) f;

                for (final Filter inner : statements) {
                    if (inner.equals(outer)) {
                        continue;
                    }

                    if (inner instanceof Filter.StartsWith) {
                        final Filter.StartsWith starts = (Filter.StartsWith) inner;

                        if (!outer.first().equals(starts.first())) {
                            continue;
                        }

                        if (FilterComparatorUtils.prefixedWith(starts.second(), outer.second())) {
                            continue root;
                        }
                    }
                }

                result.add(f);
                continue;
            }

            // all ok!
            result.add(f);
        }

        if (result.isEmpty()) {
            return FalseFilterImpl.get();
        }

        if (result.size() == 1) {
            return result.iterator().next();
        }

        return new AndFilterImpl(new ArrayList<>(result));
    }

    @Override
    public String operator() {
        return OPERATOR;
    }

    @Override
    public List<Filter> terms() {
        return statements;
    }

    public static Filter of(Filter... filters) {
        return new AndFilterImpl(Arrays.asList(filters));
    }

    @Override
    public int compareTo(Filter o) {
        if (!Filter.And.class.isAssignableFrom(o.getClass())) {
            return operator().compareTo(o.operator());
        }

        final Filter.And other = (Filter.And) o;
        return FilterComparatorUtils.compareLists(terms(), other.terms());
    }

    private final Joiner and = Joiner.on(" and ");

    @Override
    public String toDSL() {
        return "(" + and.join(statements.stream().map(Filter::toDSL).iterator()) + ")";
    }
}
