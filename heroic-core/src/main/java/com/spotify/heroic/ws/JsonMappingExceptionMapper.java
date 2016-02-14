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

import com.fasterxml.jackson.databind.JsonMappingException;
import com.spotify.heroic.common.Validation;

import javax.inject.Inject;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import java.util.function.Consumer;

public class JsonMappingExceptionMapper implements ExceptionMapper<JsonMappingException> {
    @Inject
    public JsonMappingExceptionMapper() {
    }

    @Override
    public Response toResponse(JsonMappingException e) {
        final String path = constructPath(e);

        return Response
            .status(Response.Status.BAD_REQUEST)
            .entity(new JsonErrorMessage(e.getOriginalMessage(), Response.Status.BAD_REQUEST, path))
            .type(MediaType.APPLICATION_JSON)
            .build();
    }

    private String constructPath(final JsonMappingException e) {
        final StringBuilder builder = new StringBuilder();

        final Consumer<String> field = name -> {
            if (builder.length() > 0) {
                builder.append(".");
            }

            builder.append(name);
        };

        for (final JsonMappingException.Reference reference : e.getPath()) {
            if (reference.getIndex() >= 0) {
                builder.append("[" + reference.getIndex() + "]");
            } else {
                field.accept(reference.getFieldName());
            }
        }

        if (e.getCause() instanceof Validation.MissingField) {
            final Validation.MissingField f = (Validation.MissingField) e.getCause();
            field.accept(f.getName());
        }

        return builder.toString();
    }
}
