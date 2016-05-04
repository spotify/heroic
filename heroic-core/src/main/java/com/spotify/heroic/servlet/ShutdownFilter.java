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

package com.spotify.heroic.servlet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.spotify.heroic.ws.InternalErrorMessage;
import lombok.RequiredArgsConstructor;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response.Status;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.function.Supplier;

@RequiredArgsConstructor
public class ShutdownFilter implements Filter {
    private static final String CONTENT_TYPE = "application/json; charset=UTF-8";

    private final Supplier<Boolean> stopping;
    private final ObjectMapper mapper;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(
        final ServletRequest request, final ServletResponse response, final FilterChain chain
    ) throws IOException, ServletException {
        if (!stopping.get()) {
            chain.doFilter(request, response);
            return;
        }

        final HttpServletResponse httpResponse = (HttpServletResponse) response;

        final InternalErrorMessage info =
            new InternalErrorMessage("Heroic is shutting down", Status.SERVICE_UNAVAILABLE);

        httpResponse.setStatus(Status.SERVICE_UNAVAILABLE.getStatusCode());
        httpResponse.setContentType(CONTENT_TYPE);

        // intercept request
        try (final ByteArrayOutputStream output = new ByteArrayOutputStream(4096)) {
            final OutputStreamWriter writer = new OutputStreamWriter(output, Charsets.UTF_8);
            mapper.writeValue(writer, info);
            response.setContentLength(output.size());
            output.writeTo(httpResponse.getOutputStream());
        }
    }

    @Override
    public void destroy() {
    }
};
