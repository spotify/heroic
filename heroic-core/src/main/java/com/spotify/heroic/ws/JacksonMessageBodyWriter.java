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

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

/**
 * A Jackson-based MessageBodyWriter.
 * <p>
 * This is used instead of the jackson provided ones to avoid bad default behaviours.
 */
public class JacksonMessageBodyWriter implements MessageBodyWriter<Object> {
    private final ObjectMapper mapper;

    @Inject
    public JacksonMessageBodyWriter(@Named("application/json") ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public boolean isWriteable(
        final Class<?> type, final Type genericType, final Annotation[] annotations,
        final MediaType mediaType
    ) {
        return mediaType.isCompatible(MediaType.APPLICATION_JSON_TYPE);
    }

    @Override
    public long getSize(
        final Object o, final Class<?> type, final Type genericType, final Annotation[] annotations,
        final MediaType mediaType
    ) {
        return -1;
    }

    @Override
    public void writeTo(
        final Object o, final Class<?> type, final Type genericType, final Annotation[] annotations,
        final MediaType mediaType, final MultivaluedMap<String, Object> httpHeaders,
        final OutputStream out
    ) throws IOException, WebApplicationException {
        mapper.writeValue(out, o);
    }
}
