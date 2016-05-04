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

package com.spotify.heroic.ws;

import lombok.Getter;

import javax.ws.rs.core.Response;

public class ParseErrorMessage extends ErrorMessage {
    @Getter
    private final int line;
    @Getter
    private final int col;
    @Getter
    private final int lineEnd;
    @Getter
    private final int colEnd;

    public ParseErrorMessage(
        final String message, final Response.Status status, final int line, final int col,
        final int lineEnd, final int colEnd
    ) {
        super(message, status);
        this.line = line;
        this.col = col;
        this.lineEnd = lineEnd;
        this.colEnd = colEnd;
    }

    public String getType() {
        return "parse-error";
    }
}
