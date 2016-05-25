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

import com.spotify.heroic.common.Series;

import java.util.List;

public interface Filter extends Comparable<Filter> {
    /**
     * Apply the filter to the given series.
     *
     * @param series Series to apply to.
     * @return {@code true} if filter matches the given series, {@code false} otherwise.
     */
    boolean apply(Series series);

    <T> T visit(Visitor<T> visitor);

    Filter optimize();

    String operator();

    String toDSL();

    interface MultiArgs<A> extends Filter {
        List<A> terms();
    }

    interface NoArg extends Filter {
    }

    interface OneArg<A> extends Filter {
        A first();
    }

    interface TwoArgs<A, B> extends Filter {
        A first();

        B second();
    }

    interface Visitor<T> {
        default T visitStartsWith(StartsWithFilter startsWith) {
            return defaultAction(startsWith);
        }

        default T visitHasTag(HasTagFilter hasTag) {
            return defaultAction(hasTag);
        }

        default T visitNot(NotFilter not) {
            return defaultAction(not);
        }

        default T visitTrue(TrueFilter t) {
            return defaultAction(t);
        }

        default T visitFalse(FalseFilter f) {
            return defaultAction(f);
        }

        default T visitMatchTag(MatchTagFilter matchTag) {
            return defaultAction(matchTag);
        }

        default T visitMatchKey(MatchKeyFilter matchKey) {
            return defaultAction(matchKey);
        }

        default T visitAnd(AndFilter and) {
            return defaultAction(and);
        }

        default T visitOr(OrFilter or) {
            return defaultAction(or);
        }

        default T visitRaw(RawFilter raw) {
            return defaultAction(raw);
        }

        default T visitRegex(RegexFilter regex) {
            return defaultAction(regex);
        }

        T defaultAction(Filter filter);
    }
}
