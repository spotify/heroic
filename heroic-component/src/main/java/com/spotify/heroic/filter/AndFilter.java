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
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.ObjectHasher;
import com.spotify.heroic.common.Series;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Stream;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;

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

        for (final Filter f : statements) {
            if (f instanceof NotFilter) {
                // Optimize away expressions which are always false.
                // Example: foo = bar and !(foo = bar)

                if (statements.contains(((NotFilter) f).getFilter())) {
                    return FalseFilter.get();
                }
            } else if (f instanceof StartsWithFilter) {
                // Optimize away prefixes which encompass each other.
                // Example: foo ^ hello and foo ^ helloworld -> foo ^ helloworld

                if (FilterUtils.containsPrefixedWith(statements, (StartsWithFilter) f,
                    (inner, outer) -> FilterUtils.prefixedWith(inner.getValue(),
                        outer.getValue()))) {
                    continue;
                }
            } else if (f instanceof MatchTagFilter) {
                // Optimize matchTag expressions which are always false.
                // Example: foo = bar and foo = baz

                if (FilterUtils.containsConflictingMatchTag(statements, (MatchTagFilter) f)) {
                    return FalseFilter.get();
                }
            } else if (f instanceof MatchKeyFilter) {
                // Optimize matchTag expressions which are always false.
                // Example: $key = bar and $key = baz

                if (FilterUtils.containsConflictingMatchKey(statements, (MatchKeyFilter) f)) {
                    return FalseFilter.get();
                }
            }

            result.add(f);
        }

        if (result.isEmpty()) {
            return FalseFilter.get();
        }

        if (result.size() == 1) {
            return result.iterator().next();
        }

        return new AndFilter(ImmutableList.copyOf(result));
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

    @Override
    public void hashTo(final ObjectHasher hasher) {
        hasher.putObject(this.getClass(), () -> {
            hasher.putField("statements", statements, hasher.list(hasher.with(Filter::hashTo)));
        });
    }

    public static Filter of(Filter... filters) {
        return new AndFilter(Arrays.asList(filters));
    }
}
