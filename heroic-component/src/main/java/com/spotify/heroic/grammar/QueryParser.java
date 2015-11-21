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

import java.util.List;
import java.util.Optional;

import com.google.common.base.Joiner;
import com.spotify.heroic.Query;
import com.spotify.heroic.aggregation.Aggregation;
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
     * Parse the given aggregation using the Heroic Query DSL.
     *
     * @param aggregation String to parse.
     * @return ParseException if unable to parse string.
     */
    Optional<Aggregation> parseAggregation(String aggregation);

    /**
     * Parse the given query.
     *
     * @param query String to parse.
     * @return The parse query.
     * @throws ParseException if unable to parse string.
     */
    Query parseQuery(String query);

    String stringifyQuery(Query query);

    static String escapeList(List<String> input) {
        if (input.size() == 1) {
            return escapeString(input.iterator().next());
        }

        return "[" + Joiner.on(", ").join(input.stream().map(QueryParser::escapeString).iterator())
                + "]";
    }

    static String escapeString(String input) {
        boolean quoted = false;

        final StringBuilder builder = new StringBuilder();

        for (int i = 0; i < input.length(); i++) {
            final char c = input.charAt(i);

            if (Character.isDigit(c) || ('a' <= c && c <= 'z') || ('A' < c && c <= 'Z') || c == '-'
                    || c == ':' || c == '/') {
                builder.append(c);
                continue;
            }

            quoted = true;

            switch (c) {
            case '\b':
                builder.append("\\b");
                break;
            case '\t':
                builder.append("\\t");
                break;
            case '\n':
                builder.append("\\n");
                break;
            case '\f':
                builder.append("\\f");
                break;
            case '\r':
                builder.append("\\r");
                break;
            case '"':
                builder.append("\\\"");
                break;
            case '\\':
                builder.append("\\\\");
                break;
            case '\'':
                builder.append("\\'");
                break;
            default:
                builder.append(c);
                break;
            }
        }

        if (quoted) {
            return "\"" + builder.toString() + "\"";
        }

        return builder.toString();
    }
}
