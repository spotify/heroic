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

/**
 * Utilities used when interacting with the DSL.
 */
public interface DSL {
    /**
     * Dump the given string to a DSL-based string.
     *
     * @param input Input string to quote.
     * @return A quoted string.
     */
    static String dumpString(String input) {
        if (input.isEmpty()) {
            return "\"\"";
        }

        boolean quoted = false;

        final StringBuilder builder = new StringBuilder();

        for (int i = 0; i < input.length(); i++) {
            final char c = input.charAt(i);

            if (Character.isDigit(c) || ('a' <= c && c <= 'z') || ('A' < c && c <= 'Z') ||
                c == '-' || c == ':' || c == '/' || c == '.') {
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
