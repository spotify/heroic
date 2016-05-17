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

@Data
@EqualsAndHashCode(of = {"OPERATOR", "statements"}, doNotUseGetters = true)
public class OrFilter implements Filter.MultiArgs<Filter> {
    public static final String OPERATOR = "or";

    private final List<Filter> statements;

    @Override
    public boolean apply(Series series) {
        return statements.stream().anyMatch(s -> s.apply(series));
    }

    @Override
    public <T> T visit(final Visitor<T> visitor) {
        return visitor.visitOr(this);
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

            if (o instanceof OrFilter) {
                result.addAll(((OrFilter) o).terms());
                continue;
            }

            if (o instanceof NotFilter) {
                final NotFilter not = (NotFilter) o;

                if (not.first() instanceof AndFilter) {
                    result.addAll(collapseNotAnd((AndFilter) not.first()));
                    continue;
                }
            }

            result.add(o);
        }

        return result;
    }

    private static List<Filter> collapseNotAnd(AndFilter first) {
        return ImmutableList.copyOf(
            first.terms().stream().map(t -> new NotFilter(t).optimize()).iterator());
    }

    private static Filter optimize(SortedSet<Filter> statements) {
        final SortedSet<Filter> result = new TreeSet<>();

        root:
        for (final Filter f : statements) {
            if (f instanceof NotFilter) {
                final NotFilter not = (NotFilter) f;

                if (statements.contains(not.first())) {
                    return TrueFilter.get();
                }

                result.add(f);
                continue;
            }

            if (f instanceof StartsWithFilter) {
                final StartsWithFilter outer = (StartsWithFilter) f;

                for (final Filter inner : statements) {
                    if (inner.equals(outer)) {
                        continue;
                    }

                    if (inner instanceof StartsWithFilter) {
                        final StartsWithFilter starts = (StartsWithFilter) inner;

                        if (!outer.first().equals(starts.first())) {
                            continue;
                        }

                        if (FilterComparatorUtils.prefixedWith(outer.second(), starts.second())) {
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
            return TrueFilter.get();
        }

        if (result.size() == 1) {
            return result.iterator().next();
        }

        return new OrFilter(new ArrayList<>(result));
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
    public String operator() {
        return OPERATOR;
    }

    @Override
    public List<Filter> terms() {
        return statements;
    }

    public static Filter of(Filter... filters) {
        return new OrFilter(Arrays.asList(filters));
    }

    @Override
    public int compareTo(Filter o) {
        if (!OrFilter.class.equals(o.getClass())) {
            return operator().compareTo(o.operator());
        }

        final OrFilter other = (OrFilter) o;
        return FilterComparatorUtils.compareLists(terms(), other.terms());
    }

    private final Joiner or = Joiner.on(" or ");

    @Override
    public String toDSL() {
        return "(" + or.join(statements.stream().map(Filter::toDSL).iterator()) + ")";
    }
}
