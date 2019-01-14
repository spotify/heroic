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

import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.function.BiFunction;

public class FilterUtils {
    /**
     * Return true of both a and b are non-null, and b is a prefix (but not equal to) a.
     */
    public static boolean prefixedWith(String a, String b) {
        // strict prefixes only.
        if (a.equals(b)) {
            return false;
        }

        return a.startsWith(b);
    }

    /**
     * Compare two lists of things which extend the same type <T>.
     */
    public static <T extends Comparable<T>> int compareLists(List<T> a, List<T> b) {
        final Iterator<T> left = a.iterator();
        final Iterator<T> right = b.iterator();

        while (left.hasNext()) {
            if (!right.hasNext()) {
                return 1;
            }

            final T l = left.next();
            final T r = right.next();

            final int c = l.compareTo(r);

            if (c != 0) {
                return c;
            }
        }

        if (right.hasNext()) {
            return -1;
        }

        return 0;
    }

    public static boolean containsPrefixedWith(
        final SortedSet<Filter> statements, final StartsWithFilter outer,
        final BiFunction<StartsWithFilter, StartsWithFilter, Boolean> check
    ) {
        return statements
            .stream()
            .filter(inner -> inner instanceof StartsWithFilter)
            .map(StartsWithFilter.class::cast)
            .filter(statements::contains)
            .filter(inner -> outer.getTag().equals(inner.getTag()))
            .filter(inner -> check.apply(inner, outer))
            .findFirst()
            .isPresent();
    }

    public static boolean containsConflictingMatchTag(
        final SortedSet<Filter> statements, final MatchTagFilter outer
    ) {
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
                    return true;
                }
            }
        }

        return false;
    }

    public static boolean containsConflictingMatchKey(
        final SortedSet<Filter> statements, final MatchKeyFilter outer
    ) {
        for (final Filter inner : statements) {
            if (inner.equals(outer)) {
                continue;
            }

            if (inner instanceof MatchKeyFilter) {
                final MatchKeyFilter matchKey = (MatchKeyFilter) inner;

                if (!outer.getKey().equals(matchKey.getKey())) {
                    return true;
                }
            }
        }

        return false;
    }
}
