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

package com.spotify.heroic.grammar;

import com.spotify.heroic.filter.Filter;

public interface QueryParser {
    /**
     * Parse the given filter using the Heroic Query DSL.
     *
     * @param filter String to parse.
     * @return A filter implementation.
     * @throws ParseException if unable to parse string.
     */
    Filter parseFilter(String filter);

    /**
     * Parse the given query.
     *
     * @param query String to parse.
     * @return The parse query.
     * @throws ParseException if unable to parse string.
     */
    QueryDSL parseQuery(String query);
}