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

package com.spotify.heroic.filter;

import static com.spotify.heroic.filter.FilterEncoding.filter;
import static com.spotify.heroic.filter.FilterEncoding.string;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TreeTraversingParser;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.grammar.QueryParser;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Registry of all known filters.
 * <p>
 * This is primarily used to provide serialization for Filters.
 * <p>
 * A filter can be deserialized from an object, or an array. When an object is used, the type field
 * is determined by the filters {@link com.fasterxml.jackson.annotation.JsonTypeName} annotation.
 * Otherwise it falls back to the id.
 * <p>
 * Object-based is the more universal method, but is typically more verbose. It is useful for use in
 * clients which are statically typed, since type-based deserialization is usually readily
 * supported.
 *
 * @see com.spotify.heroic.filter.Filter
 */
public class FilterRegistry {
    private final Map<String, FilterEncoding<? extends Filter>> deserializers = new HashMap<>();

    private final Map<Class<? extends Filter>, JsonSerializer<Filter>> serializers =
        new HashMap<>();

    private final HashMap<Class<? extends Filter>, String> typeMapping = new HashMap<>();
    private final HashMap<String, Class<? extends Filter>> typeNameMapping = new HashMap<>();

    public FilterRegistry() {
    }

    public <T extends Filter> void registerList(
        String id, Class<T> type, FilterEncoding<T> s
    ) {
        registerJson(id, type, s);
        register(id, type);
    }

    public <T extends Filter> void registerTwo(
        String id, Class<T> type, FilterEncoding<T> s
    ) {
        registerJson(id, type, s);
        register(id, type);
    }

    public <T extends Filter> void registerOne(
        String id, Class<T> type, FilterEncoding<T> s
    ) {
        registerJson(id, type, s);
        register(id, type);
    }

    public <T extends Filter> void registerEmpty(
        String id, Class<T> type, FilterEncoding<T> s
    ) {
        registerJson(id, type, s);
        register(id, type);
    }

    public Module module(final QueryParser parser) {
        final SimpleModule m = new SimpleModule("filter");

        for (final Map.Entry<Class<? extends Filter>, JsonSerializer<Filter>> e : this
            .serializers.entrySet()) {
            m.addSerializer(e.getKey(), e.getValue());
        }

        final FilterJsonDeserializer deserializer =
            new FilterJsonDeserializer(ImmutableMap.copyOf(deserializers), typeNameMapping, parser);
        m.addDeserializer(Filter.class, deserializer);
        return m;
    }

    @SuppressWarnings("unchecked")
    private <T extends Filter> void registerJson(
        String id, Class<T> type, FilterEncoding<T> serialization
    ) {
        serializers.put(type, new FilterJsonSerializer((FilterEncoding<Filter>) serialization));
        deserializers.put(id, serialization);
    }

    private <T extends Filter> void register(String id, Class<T> type) {
        if (typeMapping.put(type, id) != null) {
            throw new IllegalStateException("Multiple mappings for single type: " + type);
        }

        final String typeName = buildTypeId(id, type);

        if (typeNameMapping.put(typeName, type) != null) {
            throw new IllegalStateException("Multiple type names for single type: " + type);
        }
    }

    private <T extends Filter> String buildTypeId(final String id, final Class<T> type) {
        final JsonTypeName annotation = type.getAnnotation(JsonTypeName.class);

        if (annotation == null) {
            return id;
        }

        return annotation.value();
    }

    private static final class FilterJsonSerializer extends JsonSerializer<Filter> {
        private final FilterEncoding<Filter> serializer;

        @java.beans.ConstructorProperties({ "serializer" })
        public FilterJsonSerializer(final FilterEncoding<Filter> serializer) {
            this.serializer = serializer;
        }

        @Override
        public void serialize(Filter value, JsonGenerator g, SerializerProvider provider)
            throws IOException {
            g.writeStartArray();
            g.writeString(value.operator());

            final EncoderImpl s = new EncoderImpl(g);
            serializer.serialize(s, value);
            g.writeEndArray();
        }

        private static final class EncoderImpl implements FilterEncoding.Encoder {
            private final JsonGenerator generator;

            @java.beans.ConstructorProperties({ "generator" })
            public EncoderImpl(final JsonGenerator generator) {
                this.generator = generator;
            }

            @Override
            public void string(String string) throws IOException {
                generator.writeString(string);
            }

            @Override
            public void filter(Filter filter) throws IOException {
                generator.writeObject(filter);
            }
        }
    }

    /**
     * Provides both array, and object based deserialization for filters.
     */
    static class FilterJsonDeserializer extends JsonDeserializer<Filter> {
        final Map<String, FilterEncoding<? extends Filter>> deserializers;
        final HashMap<String, Class<? extends Filter>> typeNameMapping;
        final QueryParser parser;

        @java.beans.ConstructorProperties({ "deserializers", "typeNameMapping", "parser" })
        public FilterJsonDeserializer(
            final Map<String, FilterEncoding<? extends Filter>> deserializers,
            final HashMap<String, Class<? extends Filter>> typeNameMapping,
            final QueryParser parser) {
            this.deserializers = deserializers;
            this.typeNameMapping = typeNameMapping;
            this.parser = parser;
        }

        @Override
        public Filter deserialize(JsonParser p, DeserializationContext c) throws IOException {
            if (p.getCurrentToken() == JsonToken.START_ARRAY) {
                return deserializeArray(p, c);
            }

            if (p.getCurrentToken() == JsonToken.START_OBJECT) {
                return deserializeObject(p, c);
            }

            throw c.mappingException("Expected start of array or object");
        }

        private Filter deserializeArray(final JsonParser p, final DeserializationContext c)
            throws IOException {
            if (p.nextToken() != JsonToken.VALUE_STRING) {
                throw c.mappingException("Expected operator (string)");
            }

            final String operator = p.readValueAs(String.class);

            final FilterEncoding<? extends Filter> deserializer = deserializers.get(operator);

            if (deserializer == null) {
                throw c.mappingException("No such operator: " + operator);
            }

            p.nextToken();

            final FilterEncoding.Decoder d = new Decoder(p, c);

            final Filter filter;

            try {
                filter = deserializer.deserialize(d);

                if (p.getCurrentToken() != JsonToken.END_ARRAY) {
                    throw c.mappingException("Expected end of array from '" + deserializer + "'");
                }

                if (filter instanceof RawFilter) {
                    return parseRawFilter((RawFilter) filter);
                }

                return filter.optimize();
            } catch (final Exception e) {
                // use special {operator} syntax to indicate filter.
                throw JsonMappingException.wrapWithPath(e, this, "{" + operator + "}");
            }
        }

        private Filter deserializeObject(final JsonParser p, final DeserializationContext c)
            throws IOException {
            final ObjectNode object = (ObjectNode) p.readValueAs(JsonNode.class);

            final JsonNode typeNode = object.remove("type");

            if (typeNode == null) {
                throw c.mappingException("Expected 'type' field");
            }

            if (!typeNode.isTextual()) {
                throw c.mappingException("Expected 'type' to be string");
            }

            final String type = typeNode.asText();

            final Class<? extends Filter> cls = typeNameMapping.get(type);

            if (cls == null) {
                throw c.mappingException("No such type: " + type);
            }

            // use tree traversing parser to operate on the node (without 'type') again.
            final TreeTraversingParser parser = new TreeTraversingParser(object, p.getCodec());
            return parser.readValueAs(cls);
        }

        private Filter parseRawFilter(RawFilter filter) {
            return parser.parseFilter(filter.filter());
        }

        private static final class Decoder implements FilterEncoding.Decoder {
            private final JsonParser parser;
            private final DeserializationContext c;

            private int index = 0;

            @java.beans.ConstructorProperties({ "parser", "c" })
            public Decoder(final JsonParser parser, final DeserializationContext c) {
                this.parser = parser;
                this.c = c;
            }

            @Override
            public Optional<String> string() throws IOException {
                final int index = this.index++;

                if (parser.getCurrentToken() == JsonToken.END_ARRAY) {
                    return Optional.empty();
                }

                if (parser.getCurrentToken() != JsonToken.VALUE_STRING) {
                    throw c.mappingException("Expected string");
                }

                final String string;

                try {
                    string = parser.getValueAsString();
                } catch (final JsonMappingException e) {
                    throw JsonMappingException.wrapWithPath(e, this, index);
                }

                parser.nextToken();
                return Optional.of(string);
            }

            @Override
            public Optional<Filter> filter() throws IOException {
                final int index = this.index++;

                if (parser.getCurrentToken() == JsonToken.END_ARRAY) {
                    return Optional.empty();
                }

                if (parser.getCurrentToken() != JsonToken.START_ARRAY) {
                    throw c.mappingException("Expected start of new filter expression");
                }

                final Filter filter;

                try {
                    filter = parser.readValueAs(Filter.class);
                } catch (final JsonMappingException e) {
                    throw JsonMappingException.wrapWithPath(e, this, index);
                }

                parser.nextToken();
                return Optional.of(filter);
            }
        }
    }

    public static FilterRegistry registry() {
        final FilterRegistry registry = new FilterRegistry();

        registry.registerList(AndFilter.OPERATOR, AndFilter.class,
            new MultiArgumentsFilterBase<>(AndFilter::create, AndFilter::filters, filter()));

        registry.registerList(OrFilter.OPERATOR, OrFilter.class,
            new MultiArgumentsFilterBase<>(OrFilter::create, OrFilter::filters, filter()));

        registry.registerOne(NotFilter.OPERATOR, NotFilter.class,
            new OneArgumentFilterEncoding<>(NotFilter::create, NotFilter::filter, filter()));

        registry.registerTwo(MatchKeyFilter.OPERATOR, MatchKeyFilter.class,
            new OneArgumentFilterEncoding<>(MatchKeyFilter::create, MatchKeyFilter::key, string()));

        registry.registerTwo(MatchTagFilter.OPERATOR, MatchTagFilter.class,
            new TwoArgumentFilterEncoding<>(MatchTagFilter::create, MatchTagFilter::tag,
                MatchTagFilter::value, string(), string()));

        registry.registerOne(HasTagFilter.OPERATOR, HasTagFilter.class,
            new OneArgumentFilterEncoding<>(HasTagFilter::create, HasTagFilter::tag, string()));

        registry.registerTwo(StartsWithFilter.OPERATOR, StartsWithFilter.class,
            new TwoArgumentFilterEncoding<>(StartsWithFilter::create, StartsWithFilter::tag,
                StartsWithFilter::value, string(), string()));

        registry.registerTwo(RegexFilter.OPERATOR, RegexFilter.class,
            new TwoArgumentFilterEncoding<>(RegexFilter::create, RegexFilter::tag,
                RegexFilter::value, string(), string()));

        registry.registerEmpty(TrueFilter.OPERATOR, TrueFilter.class,
            new NoArgumentFilterBase<>(TrueFilter::get));

        registry.registerEmpty(FalseFilter.OPERATOR, FalseFilter.class,
            new NoArgumentFilterBase<>(FalseFilter::get));

        registry.registerOne(RawFilter.OPERATOR, RawFilter.class,
            new OneArgumentFilterEncoding<>(RawFilter::create, RawFilter::filter, string()));

        return registry;
    }
}
