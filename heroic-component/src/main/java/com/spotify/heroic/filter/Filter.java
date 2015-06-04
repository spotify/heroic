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

public interface Filter extends Comparable<Filter> {
    public interface MultiArgs<A> extends Filter {
        public List<A> terms();
    }

    public interface NoArg extends Filter {
        Filter invert();
    }

    public interface OneArg<A> extends Filter {
        public A first();
    }

    public interface TwoArgs<A, B> extends Filter {
        public A first();

        public B second();
    }

    /**
     * Concrete interfaces for filters.
     *
     * These are necessary when writing converters that typically check instance types.
     **/
    public interface Or extends MultiArgs<Filter> {
    }

    public interface And extends MultiArgs<Filter> {
    }

    public interface True extends NoArg {
    }

    public interface False extends NoArg {
    }

    public interface HasTag extends OneArg<String> {
    }

    public interface MatchKey extends OneArg<String> {
    }

    public interface MatchTag extends TwoArgs<String, String> {
    }

    public interface Not extends OneArg<Filter> {
    }

    public interface StartsWith extends TwoArgs<String, String> {
    }

    public interface Regex extends TwoArgs<String, String> {
    }

    public interface Raw extends OneArg<String> {
    }

    public Filter optimize();

    public String operator();
}
