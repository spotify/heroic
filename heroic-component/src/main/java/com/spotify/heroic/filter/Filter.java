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

import java.util.List;

import com.spotify.heroic.common.Series;

public interface Filter extends Comparable<Filter> {
    /**
     * Apply the filter to the given series.
     *
     * @param series Series to apply to.
     * @return {@code true} if filter matches the given series, {@code false} otherwise.
     */
    boolean apply(Series series);

    interface MultiArgs<A> extends Filter {
        List<A> terms();
    }

    interface NoArg extends Filter {
        Filter invert();
    }

    interface OneArg<A> extends Filter {
        A first();
    }

    interface TwoArgs<A, B> extends Filter {
        A first();

        B second();
    }

    /**
     * Concrete interfaces for filters.
     *
     * These are necessary when writing converters that typically check instance types.
     **/
    interface Or extends MultiArgs<Filter> {
    }

    interface And extends MultiArgs<Filter> {
    }

    interface True extends NoArg {
    }

    interface False extends NoArg {
    }

    interface HasTag extends OneArg<String> {
    }

    interface MatchKey extends OneArg<String> {
    }

    interface MatchTag extends TwoArgs<String, String> {
    }

    interface Not extends OneArg<Filter> {
    }

    interface StartsWith extends TwoArgs<String, String> {
    }

    interface Regex extends TwoArgs<String, String> {
    }

    interface Raw extends OneArg<String> {
    }

    Filter optimize();

    String operator();

    String toDSL();
}
