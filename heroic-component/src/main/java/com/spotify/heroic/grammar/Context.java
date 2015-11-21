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

import lombok.Data;

@Data
public class Context {
    private final int line;
    private final int col;
    private final int lineEnd;
    private final int colEnd;

    public ParseException error(final String message) {
        return new ParseException(message, null, line, col, lineEnd, colEnd);
    }

    public ParseException error(final Exception cause) {
        return new ParseException(cause.getMessage(), cause, line, col, lineEnd, colEnd);
    }

    public ParseException castError(final Object from, final Class<?> to) {
        return new ParseException(String.format("%s cannot be cast to %s", from, name(to)), null,
                line, col, lineEnd, colEnd);
    }

    public ParseException castError(final Object from, final Object to) {
        return new ParseException(
                String.format("%s cannot be cast to a compatible type of %s", from, to), null, line,
                col, lineEnd, colEnd);
    }

    public Context join(final Context o) {
        return new Context(Math.min(getLine(), o.getLine()), Math.min(getCol(), o.getCol()),
                Math.max(getLineEnd(), o.getLineEnd()), Math.max(getColEnd(), o.getColEnd()));
    }

    public static Context empty() {
        return new Context(-1, -1, -1, -1);
    }

    static String name(Class<?> type) {
        final ValueName name = type.getAnnotation(ValueName.class);

        if (name != null) {
            return name.value();
        }

        return type.getSimpleName();
    }
}
