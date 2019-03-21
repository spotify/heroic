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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
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
import org.apache.commons.lang3.StringUtils;

@AutoValue
public abstract class OrFilter implements Filter {
    @JsonCreator
    public static OrFilter create(@JsonProperty("filters") List<Filter> filters) {
        return new AutoValue_OrFilter(filters);
    }

    public static final String OPERATOR = "or";
    abstract List<Filter> filters();

    @Override
    public boolean apply(Series series) {
        return filters().stream().anyMatch(s -> s.apply(series));
    }

    @Override
    public <T> T visit(final Visitor<T> visitor) {
        return visitor.visitOr(this);
    }

    @Override
    public Filter optimize() {
        return optimize(flatten(this.filters()));
    }

    static SortedSet<Filter> flatten(final Collection<Filter> filters) {
        final SortedSet<Filter> result = new TreeSet<>();

        filters.stream().flatMap(f -> f.optimize().visit(new Visitor<Stream<Filter>>() {
            @Override
            public Stream<Filter> visitOr(final OrFilter or) {
                return or.terms().stream().map(Filter::optimize);
            }

            @Override
            public Stream<Filter> visitNot(final NotFilter not) {
                // check for De Morgan's
                return not.filter().visit(new Filter.Visitor<Stream<Filter>>() {
                    @Override
                    public Stream<Filter> visitAnd(final AndFilter and) {
                        return and.terms().stream().map(f -> NotFilter.of(f).optimize());
                    }

                    @Override
                    public Stream<Filter> defaultAction(final Filter filter) {
                        return Stream.of(not.optimize());
                    }
                });
            }

            @Override
            public Stream<Filter> defaultAction(final Filter filter) {
                return Stream.of(filter);
            }
        })).forEach(result::add);

        return result;
    }

    static Filter optimize(final SortedSet<Filter> filters) {
        final SortedSet<Filter> result = new TreeSet<>();

        for (final Filter f : filters) {
            if (f instanceof NotFilter) {
                // Optimize away expressions which are always true.
                // Example: foo = bar or !(foo = bar)

                if (filters.contains(((NotFilter) f).filter())) {
                    return TrueFilter.get();
                }
            } else if (f instanceof StartsWithFilter) {
                // Optimize away prefixes which encompass each other.
                // Example: foo ^ hello or foo ^ helloworld -> foo ^ hello

                if (FilterUtils.containsPrefixedWith(filters, (StartsWithFilter) f,
                    (inner, outer) -> FilterUtils.prefixedWith(outer.value(),
                        inner.value()))) {
                    continue;
                }
            }

            result.add(f);
        }

        if (result.isEmpty()) {
            return TrueFilter.get();
        }

        if (result.size() == 1) {
            return result.iterator().next();
        }

        return OrFilter.create(ImmutableList.copyOf(result));
    }

    @Override
    public String toString() {
        final List<String> parts = new ArrayList<>(filters().size() + 1);
        parts.add(OPERATOR);

        for (final Filter statement : filters()) {
            parts.add(statement.toString());
        }

        return "[" + StringUtils.join(parts, ", ") + "]";
    }

    @Override
    public String operator() {
        return OPERATOR;
    }

    public List<Filter> terms() {
        return filters();
    }

    public static Filter of(Filter... filters) {
        return OrFilter.create(Arrays.asList(filters));
    }

    @Override
    public int compareTo(Filter o) {
        if (!OrFilter.class.isAssignableFrom(o.getClass())) {
            return operator().compareTo(o.operator());
        }

        final OrFilter other = (OrFilter) o;
        return FilterUtils.compareLists(terms(), other.terms());
    }

    private final Joiner or = Joiner.on(" or ");

    @Override
    public String toDSL() {
        return "(" + or.join(filters().stream().map(Filter::toDSL).iterator()) + ")";
    }

    @Override
    public void hashTo(final ObjectHasher hasher) {
        hasher.putObject(this.getClass(), () -> {
            hasher.putField("filters", filters(), hasher.list(hasher.with(Filter::hashTo)));
        });
    }
}
