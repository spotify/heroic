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

import com.google.common.base.Joiner;
import com.spotify.heroic.common.Series;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Stream;

@Data
@EqualsAndHashCode(of = {"OPERATOR", "statements"}, doNotUseGetters = true)
public class AndFilter implements Filter {
    public static final String OPERATOR = "and";

    private final List<Filter> statements;

    @Override
    public boolean apply(Series series) {
        return statements.stream().allMatch(s -> s.apply(series));
    }

    @Override
    public <T> T visit(final Visitor<T> visitor) {
        return visitor.visitAnd(this);
    }

    @Override
    public String toString() {
        final List<String> parts = new ArrayList<>(statements.size() + 1);
        parts.add(OPERATOR);

        for (final Filter statement : statements) {
            parts.add(statement.toString());
        }

        return "[" + StringUtils.join(parts, ", ") + "]";
    }

    @Override
    public Filter optimize() {
        return optimize(flatten(this.statements));
    }

    static SortedSet<Filter> flatten(final Collection<Filter> statements) {
        final SortedSet<Filter> result = new TreeSet<>();

        statements.stream().flatMap(f -> f.optimize().visit(new Filter.Visitor<Stream<Filter>>() {
            @Override
            public Stream<Filter> visitAnd(final AndFilter and) {
                return and.terms().stream().map(Filter::optimize);
            }

            @Override
            public Stream<Filter> visitNot(final NotFilter not) {
                // check for De Morgan's
                return not.getFilter().visit(new Filter.Visitor<Stream<Filter>>() {
                    @Override
                    public Stream<Filter> visitOr(final OrFilter or) {
                        return or.terms().stream().map(f -> NotFilter.of(f).optimize());
                    }

                    @Override
                    public Stream<Filter> defaultAction(final Filter filter) {
                        return Stream.of(not.optimize());
                    }
                });
            }

            @Override
            public Stream<Filter> defaultAction(final Filter filter) {
                return Stream.of(filter.optimize());
            }
        })).forEach(result::add);

        return result;
    }

    static Filter optimize(final SortedSet<Filter> statements) {
        final SortedSet<Filter> result = new TreeSet<>();

        root:
        for (final Filter f : statements) {
            if (f instanceof NotFilter) {
                final NotFilter not = (NotFilter) f;

                // always false
                if (statements.contains(not.getFilter())) {
                    return FalseFilter.get();
                }

                result.add(f);
                continue;
            }

            /**
             * If there exists two MatchTag statements, but they check for different values.
             */
            if (f instanceof MatchTagFilter) {
                final MatchTagFilter outer = (MatchTagFilter) f;

                for (final Filter inner : statements) {
                    if (inner.equals(outer)) {
                        continue;
                    }

                    if (inner instanceof MatchTagFilter) {
                        final MatchTagFilter matchTag = (MatchTagFilter) inner;

                        if (!outer.getTag().equals(matchTag.getTag())) {
                            continue;
                        }

                        // always false
                        if (!outer.getValue().equals(matchTag.getValue())) {
                            return FalseFilter.get();
                        }
                    }
                }

                result.add(f);
                continue;
            }

            // optimize away prefixes which encompass eachother.
            // Example: foo ^ hello and foo ^ helloworld -> foo ^ helloworld
            if (f instanceof StartsWithFilter) {
                if (FilterUtils.containsPrefixedWith(statements, (StartsWithFilter) f,
                    (inner, outer) -> FilterUtils.prefixedWith(inner.getValue(),
                        outer.getValue()))) {
                    continue;
                }

                result.add(f);
                continue;
            }

            // all ok!
            result.add(f);
        }

        if (result.isEmpty()) {
            return FalseFilter.get();
        }

        if (result.size() == 1) {
            return result.iterator().next();
        }

        return new AndFilter(new ArrayList<>(result));
    }

    @Override
    public String operator() {
        return OPERATOR;
    }

    public List<Filter> terms() {
        return statements;
    }

    @Override
    public int compareTo(Filter o) {
        if (!AndFilter.class.equals(o.getClass())) {
            return operator().compareTo(o.operator());
        }

        final AndFilter other = (AndFilter) o;
        return FilterUtils.compareLists(terms(), other.terms());
    }

    private final Joiner and = Joiner.on(" and ");

    @Override
    public String toDSL() {
        return "(" + and.join(statements.stream().map(Filter::toDSL).iterator()) + ")";
    }

    public static Filter of(Filter... filters) {
        return new AndFilter(Arrays.asList(filters));
    }
}
