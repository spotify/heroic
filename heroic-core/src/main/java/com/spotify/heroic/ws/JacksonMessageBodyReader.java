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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.common.Validation;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

/**
 * A JacksonMessageBodyWriter-based MessageBodyWriter.
 * <p>
 * This is used instead of the provided one to avoid de-serializing empty bodies as null.
 */
public class JacksonMessageBodyReader implements MessageBodyReader<Object> {
    private final ObjectMapper mapper;

    @Inject
    public JacksonMessageBodyReader(@Named("application/json") ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public boolean isReadable(
        final Class<?> type, final Type genericType, final Annotation[] annotations,
        final MediaType mediaType
    ) {
        return mediaType.isCompatible(MediaType.APPLICATION_JSON_TYPE);
    }

    @Override
    public Object readFrom(
        final Class<Object> type, final Type genericType, final Annotation[] annotations,
        final MediaType mediaType, final MultivaluedMap<String, String> httpHeaders,
        final InputStream in
    ) throws IOException, WebApplicationException {
        final JsonParser parser = mapper.getFactory().createParser(in);

        /* no json in request */
        if (parser.nextToken() == null) {
            throw new Validation.MissingBody("empty body");
        }

        return parser.readValueAs(type);
    }
}
