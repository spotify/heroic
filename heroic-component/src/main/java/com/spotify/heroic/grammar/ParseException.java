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
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class ParseException extends RuntimeException {
    private static final long serialVersionUID = -7313640439644659488L;

    private final int line;
    private final int col;
    private final int lineEnd;
    private final int colEnd;

    public ParseException(String message, Throwable cause, int line, int col) {
        this(message, cause, line, col, line, col);
    }

    public ParseException(
        String message, Throwable cause, int line, int col, int lineEnd, int colEnd
    ) {
        super(String.format("%d:%d: %s", line, col, message), cause);
        this.line = line;
        this.col = col;
        this.lineEnd = lineEnd;
        this.colEnd = colEnd;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
