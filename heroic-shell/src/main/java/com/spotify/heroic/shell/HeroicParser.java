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

import com.google.common.collect.ImmutableList;
import lombok.Data;
import org.jline.reader.EOFError;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Parser implementation for HeroicShell.
 *
 * <p>Supports the following features:
 *
 * <ul><li>Quoting (single or double)</li>
 *
 * <li>Escapes within quotes: \t, \n, and \uFFFFF (unicode)</li>
 *
 * <li>Comments</li>
 *
 * <li>Line continuations with backticks</li>
 *
 * <li>Multiple statements per line</li></ul>
 */
public class HeroicParser implements Parser {
    private static final char DOUBLE_QUOTE = '"';
    private static final char SINGLE_QUOTE = '\'';
    private static final char BACKTICK = '\\';
    private static final char SPACE = ' ';
    private static final char TAB = '\t';
    private static final char NL = '\n';
    private static final char HASH = '#';
    private static final char SEMI_COLON = ';';

    private static final int UNICODE_WIDTH = 4;

    @Override
    public ParsedLine parse(
        final String input, final int cursor, final ParseContext context
    ) throws SyntaxError {
        final ParserState state = new ParserState(input, cursor);

        while (state.hasMore()) {
            if (state.currentIsWhitespace()) {
                state.consumeWhitespace();
                continue;
            }

            if (state.match(DOUBLE_QUOTE)) {
                state.consumeQuote(DOUBLE_QUOTE);
                continue;
            }

            if (state.match(SINGLE_QUOTE)) {
                state.consumeQuote(SINGLE_QUOTE);
                continue;
            }

            state.append(state.current());
        }

        return state.toParsed();
    }

    public static List<List<String>> splitCommands(final List<String> raw) {
        final ImmutableList.Builder<List<String>> output = ImmutableList.builder();

        final List<String> words = new ArrayList<>();

        for (final String word : raw) {
            if (word == null) {
                output.add(ImmutableList.copyOf(words));
                words.clear();
                continue;
            }

            words.add(word);
        }

        if (!words.isEmpty()) {
            output.add(ImmutableList.copyOf(words));
        }

        return output.build();
    }

    @Data
    private static class Parsed implements ParsedLine {
        private final String word;
        private final int wordCursor;
        private final int wordIndex;
        private final List<String> words;
        private final String line;
        private final int cursor;

        @Override
        public String word() {
            return word;
        }

        @Override
        public int wordCursor() {
            return wordCursor;
        }

        @Override
        public int wordIndex() {
            return wordIndex;
        }

        @Override
        public List<String> words() {
            return words;
        }

        @Override
        public String line() {
            return line;
        }

        @Override
        public int cursor() {
            return cursor;
        }
    }

    private static class ParserState {
        private int line = 0;
        private int col = -1;
        private int index = -1;

        boolean inQuote = false;
        boolean inComment = false;

        private int wordIndex = -1;
        private int wordCursor = -1;

        private final List<String> words = new ArrayList<>();
        private StringBuffer wordBuffer = new StringBuffer();

        final String input;
        final char[] chars;
        final int cursor;

        ParserState(final String input, final int cursor) {
            this.input = input;
            this.chars = input.toCharArray();
            this.cursor = cursor;
        }

        boolean hasMore() {
            inComment = false;

            while (index < chars.length) {
                if (inQuote && index + 1 >= chars.length) {
                    throw new EOFError(line, col, "open quote");
                }

                add(1);

                if (index >= chars.length) {
                    return false;
                }

                if (match(BACKTICK)) {
                    if (index + 1 == chars.length) {
                        throw new EOFError(line, col, "abrupt end");
                    }
                }

                if (!inQuote && !inComment) {
                    if (match(SEMI_COLON)) {
                        checkWord();
                        words.add(null);
                        continue;
                    }

                    if (match(HASH)) {
                        inComment = true;
                        continue;
                    }
                }

                if (match(NL)) {
                    inComment = false;
                    eol();
                    continue;
                }

                if (match(BACKTICK, NL)) {
                    inComment = false;
                    eol();
                    add(1);
                    continue;
                }

                if (!inComment) {
                    break;
                }
            }

            return index < chars.length;
        }

        boolean match(final char first, final char... rest) {
            if (chars[index] != first) {
                return false;
            }

            if (rest.length > 0) {
                if (index < 0 || index + rest.length >= chars.length) {
                    return false;
                }

                for (int i = 0; i < rest.length; i++) {
                    if (chars[index + 1 + i] != rest[i]) {
                        return false;
                    }
                }
            }

            return true;
        }

        void eol() {
            line++;
            col = -1;
        }

        void add(int n) {
            if (n < 0) {
                throw new IllegalArgumentException();
            }

            if (index + n >= chars.length) {
                n = chars.length - index;
            }

            if (n <= 0) {
                return;
            }

            if (index < cursor && index + n >= cursor) {
                wordIndex = words.size();
                wordCursor = wordBuffer.length();
            }

            index += n;
            col += n;
        }

        void consumeQuote(final char quote) {
            inQuote = true;

            while (hasMore()) {
                final char c = chars[index];

                if (c == BACKTICK) {
                    final char esc = current(1, this::escapeError);

                    switch (esc) {
                        case 'n':
                            append(NL);
                            break;
                        case 't':
                            append(TAB);
                            break;
                        case 'u':
                            append(parseUnicode());
                            add(UNICODE_WIDTH);
                            break;
                        default:
                            if (esc != quote) {
                                throw syntaxError(
                                    String.format("illegal escape sequence (%c)", esc));
                            }

                            append(esc);
                            break;
                    }

                    add(1);
                    continue;
                }

                if (c == quote) {
                    inQuote = false;
                    break;
                }

                append(c);
            }

            if (inQuote) {
                throw syntaxError(String.format("mismatched quote (%c)", quote));
            }
        }

        char current() {
            return current(0, () -> syntaxError("no more input"));
        }

        <X extends RuntimeException> char current(int offset, Supplier<X> error) {
            final int i = index + offset;

            if (i < 0 || i >= chars.length) {
                throw error.get();
            }

            return chars[i];
        }

        char parseUnicode() {
            if (index + UNICODE_WIDTH + 1 >= chars.length) {
                throw syntaxError("unicode sequence too short");
            }

            int cp = 0;

            for (int i = 0; i < UNICODE_WIDTH; i++) {
                final char in = chars[index + 2 + i];

                final long v = Character.digit(in, 16);

                if (v < 0) {
                    throw syntaxError(
                        String.format("non-hex character (%c) in unicode escape sequence", in));
                }

                cp |= v << ((UNICODE_WIDTH - i - 1) * 4);
            }

            final char[] result = Character.toChars(cp);

            if (result.length != 1) {
                throw syntaxError("codepoint does not correspond to a single unicode character");
            }

            return result[0];
        }

        void append(final char c) {
            wordBuffer.append(c);
        }

        String word() {
            if (wordIndex != -1 && wordIndex < words.size()) {
                return words.get(wordIndex);
            }

            return "";
        }

        Parsed toParsed() {
            checkWord();
            return new Parsed(word(), wordCursor, wordIndex, words, input, cursor);
        }

        void consumeWhitespace() {
            checkWord();

            while (index < chars.length) {
                final char c = chars[index + 1];

                if (c == SPACE || c == TAB) {
                    add(1);
                    continue;
                }

                break;
            }
        }

        boolean currentIsWhitespace() {
            final char c = chars[index];
            return c == SPACE || c == TAB;
        }

        void checkWord() {
            if (wordBuffer.length() > 0) {
                words.add(wordBuffer.toString());
                wordBuffer = new StringBuffer();
            }
        }

        SyntaxError escapeError() {
            return syntaxError("unexpected end of escape sequence");
        }

        SyntaxError syntaxError(final String error) {
            return new SyntaxError(line, col, input, error);
        }
    }
}
