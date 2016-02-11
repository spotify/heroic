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

package com.spotify.heroic.jetty;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.spotify.heroic.ws.InternalErrorMessage;
import lombok.RequiredArgsConstructor;
import org.eclipse.jetty.server.Dispatcher;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.handler.ErrorHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

@RequiredArgsConstructor
public class JettyJSONErrorHandler extends ErrorHandler {
    private static final String CONTENT_TYPE = "application/json; charset=UTF-8";

    private final ObjectMapper mapper;

    @Override
    public void handle(
        String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response
    ) throws IOException {
        baseRequest.setHandled(true);
        response.setContentType(CONTENT_TYPE);

        final String message;
        final javax.ws.rs.core.Response.Status status;

        if (response instanceof Response) {
            final Response r = (Response) response;
            status = javax.ws.rs.core.Response.Status.fromStatusCode(r.getStatus());
        } else {
            status = javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
        }

        final Throwable cause = (Throwable) request.getAttribute(Dispatcher.ERROR_EXCEPTION);

        if (cause != null && cause.getMessage() != null) {
            message = cause.getMessage();
        } else if (cause instanceof NullPointerException) {
            message = "NPE";
        } else {
            message = status.getReasonPhrase();
        }

        final InternalErrorMessage info = new InternalErrorMessage(message, status);

        response.setStatus(status.getStatusCode());

        try (final ByteArrayOutputStream output = new ByteArrayOutputStream(4096)) {
            final OutputStreamWriter writer = new OutputStreamWriter(output, Charsets.UTF_8);
            mapper.writeValue(writer, info);
            response.setContentLength(output.size());
            output.writeTo(response.getOutputStream());
        }
    }
}
