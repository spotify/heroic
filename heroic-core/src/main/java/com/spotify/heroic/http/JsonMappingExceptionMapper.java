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

package com.spotify.heroic.http;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import com.fasterxml.jackson.databind.JsonMappingException;

@Provider
public class JsonMappingExceptionMapper implements ExceptionMapper<JsonMappingException> {
    @Override
    public Response toResponse(JsonMappingException e) {
        final String path = constructPath(e);

        return Response.status(Response.Status.BAD_REQUEST)
                .entity(new JsonErrorMessage(e.getOriginalMessage(),
                        Response.Status.BAD_REQUEST, path))
                .type(MediaType.APPLICATION_JSON).build();
    }

    private String constructPath(final JsonMappingException e) {
        final StringBuilder builder = new StringBuilder();

        boolean lastIndex = false;

        for (final JsonMappingException.Reference reference : e.getPath()) {
            if (reference.getIndex() >= 0) {
                builder.append("[" + reference.getIndex() + "]");
                lastIndex = true;
                continue;
            }

            if (builder.length() > 0 && !lastIndex) {
                builder.append(".");
            }

            builder.append(reference.getFieldName());
            lastIndex = false;
        }

        return builder.toString();
    }
}
