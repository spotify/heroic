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

package com.spotify.heroic.shell;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import jersey.repackaged.com.google.common.collect.ImmutableMap;

import com.google.common.collect.ImmutableList;

public class QuoteParser {
    private static final char DOUBLE_QUOTE = '"';
    private static final char SINGLE_QUOTE = '\'';
    private static final char BACKTICK = '\\';
    private static final char SPACE = ' ';
    private static final char TAB = '\t';
    private static final char NL = '\n';

    // @formatter:off
    private static final Map<Character, Integer> hexmap = ImmutableMap.<Character, Integer>builder()
            .put('0', 0).put('1', 1).put('2', 2).put('3', 3).put('4', 4).put('5', 5).put('6', 6)
            .put('7', 7).put('8', 8).put('9', 9).put('a', 10).put('A', 10).put('b', 11).put('B', 11)
            .put('c', 12).put('C', 12).put('d', 13).put('D', 13).put('e', 14).put('E', 14).put('f', 15).put('F', 15)
            .build();
    // @formatter:on

    public static List<String> parse(String input) throws QuoteParserException {
        boolean openSingle = false;
        boolean openDouble = false;
        boolean escapeNext = false;
        boolean whitespaceBlock = false;
        int unicodeChar = 0;
        final char[] unicode = new char[4];

        StringBuffer buffer = new StringBuffer();

        final List<String> parts = new ArrayList<>();

        int pos = 0;

        for (final char c : input.toCharArray()) {
            ++pos;

            if (unicodeChar > 0) {
                unicode[4 - unicodeChar--] = c;

                if (unicodeChar == 0) {
                    buffer.append(parseUnicodeChar(unicode, pos));
                }

                continue;
            }

            if (escapeNext) {
                escapeNext = false;

                if (c == 'n') {
                    buffer.append(NL);
                    continue;
                }

                if (c == 't') {
                    buffer.append(TAB);
                    continue;
                }

                if (c == 'u') {
                    unicodeChar = 4;
                    continue;
                }

                buffer.append(c);
                continue;
            }

            if (whitespaceBlock && !isWhitespace(c)) {
                whitespaceBlock = false;
                parts.add(buffer.toString());
                buffer = new StringBuffer();
            }

            if (c == BACKTICK) {
                escapeNext = true;
                continue;
            }

            if (c == SINGLE_QUOTE && !openDouble) {
                openSingle = !openSingle;
                continue;
            }

            if (c == DOUBLE_QUOTE && !openSingle) {
                openDouble = !openDouble;
                continue;
            }

            if (openSingle || openDouble) {
                buffer.append(c);
                continue;
            }

            if (isWhitespace(c)) {
                whitespaceBlock = true;
                continue;
            }

            buffer.append(c);
        }

        if (openSingle)
            throw new QuoteParserException(pos + ": input ended with open single quote");

        if (openDouble)
            throw new QuoteParserException(pos + ": input ended with open double quote");

        if (escapeNext)
            throw new QuoteParserException(pos + ": input ended with open escape");

        if (unicodeChar > 0)
            throw new QuoteParserException(pos + ": input ended with open unicode escape sequence");

        if (buffer.length() > 0)
            parts.add(buffer.toString());

        return ImmutableList.copyOf(parts);
    }

    private static char parseUnicodeChar(char[] unicode, int pos) throws QuoteParserException {
        int codepoint = 0;

        for (int i = 0; i < unicode.length; i++) {
            final char in = unicode[i];

            final Integer v = hexmap.get(in);

            if (v == null) {
                throw new QuoteParserException(pos + ": non-hex character in unicode escape sequence");
            }

            codepoint += v << ((unicode.length - i - 1) * 4);
        }

        final char[] result = Character.toChars(codepoint);

        if (result.length != 1) {
            throw new QuoteParserException(pos + ": codepoint does not correspond to a single unicode character");
        }

        return result[0];
    }

    public static boolean isWhitespace(char c) {
        return c == SPACE || c == TAB;
    }
}
