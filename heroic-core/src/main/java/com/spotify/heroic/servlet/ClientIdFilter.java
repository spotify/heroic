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
import com.google.common.base.Strings;
import com.spotify.heroic.http.CoreHttpContextFactory;
import com.spotify.heroic.requestfilters.MandatoryClientIdUtil.InfractionSeverity;
import com.spotify.heroic.ws.MissingClientIdErrorMessage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response.Status;

public class ClientIdFilter implements Filter {

    private static final String CONTENT_TYPE = "application/json; charset=UTF-8";

    private final InfractionSeverity severity;
    private final ObjectMapper mapper;

    public ClientIdFilter(final InfractionSeverity severity, final ObjectMapper mapper) {
        this.severity = severity;
        this.mapper = mapper;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(
        final ServletRequest request, final ServletResponse response, final FilterChain chain
    ) throws IOException, ServletException {

        try {
            if (isPermitted(request, response, chain)) {
                return;
            }

            var httpResponse = (HttpServletResponse) response;

            var httpStatus = severity == InfractionSeverity.REJECT ? Status.BAD_REQUEST : Status.OK;

            httpResponse.setStatus(httpStatus.getStatusCode());
            httpResponse.setContentType(CONTENT_TYPE);

            // We always write a warning
            try (final ByteArrayOutputStream output = new ByteArrayOutputStream(4096)) {
                var writer = new OutputStreamWriter(output, Charsets.UTF_8);
                mapper.writeValue(writer, new MissingClientIdErrorMessage());
                response.setContentLength(output.size());
                output.writeTo(response.getOutputStream());
            }
        } finally {
            chain.doFilter(request, response);
        }
    }

    private boolean isPermitted(ServletRequest request, ServletResponse response,
        FilterChain chain) throws IOException, ServletException {
        if (severity == InfractionSeverity.PERMIT) {
            return true;
        }

        if (request.getClass().isAssignableFrom(HttpServletRequest.class)) {

            var req = (HttpServletRequest) request;
            var maybeClientId = CoreHttpContextFactory.create(req).getClientId();
            var clientId = maybeClientId.orElse("");

            return !Strings.isNullOrEmpty(clientId);
        }

        return false;
    }

    @Override
    public void destroy() {
    }
};
