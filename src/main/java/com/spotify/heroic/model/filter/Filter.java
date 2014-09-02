package com.spotify.heroic.model.filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(using = Filter.Deserializer.class)
public interface Filter {
    public interface FilterDeserializer<T> {
        public T deserialize(JsonParser p, DeserializationContext c)
                throws IOException, JsonProcessingException;
    }

    public abstract static class TagAndValueDeserializer<T> implements
            FilterDeserializer<T> {
        @Override
        public T deserialize(JsonParser p, DeserializationContext c)
                throws IOException, JsonProcessingException {
            final String tag;

            {
                if (p.nextToken() != JsonToken.VALUE_STRING)
                    throw c.mappingException("Expected string (tag)");

                tag = p.readValueAs(String.class);
            }

            final String value;

            {
                if (p.nextToken() != JsonToken.VALUE_STRING)
                    throw c.mappingException("Expected string (value)");

                value = p.readValueAs(String.class);
            }

            if (p.nextToken() != JsonToken.END_ARRAY)
                throw c.mappingException("Expected end of array");

            return build(tag, value);
        }

        protected abstract T build(String tag, String value);
    }

    public static FilterDeserializer<MatchTagFilter> MATCH_TAG = new TagAndValueDeserializer<MatchTagFilter>() {
        @Override
        public MatchTagFilter build(String tag, String value) {
            return new MatchTagFilter(tag, value);
        }
    };

    public static FilterDeserializer<StartsWithFilter> STARTS_WITH = new TagAndValueDeserializer<StartsWithFilter>() {
        @Override
        public StartsWithFilter build(String tag, String value) {
            return new StartsWithFilter(tag, value);
        }
    };

    public static FilterDeserializer<RegexFilter> REGEX = new TagAndValueDeserializer<RegexFilter>() {
        @Override
        public RegexFilter build(String tag, String value) {
            return new RegexFilter(tag, value);
        }
    };

    public static FilterDeserializer<HasTagFilter> HAS_TAG = new FilterDeserializer<HasTagFilter>() {
        @Override
        public HasTagFilter deserialize(JsonParser p, DeserializationContext c)
                throws IOException, JsonProcessingException {
            final String tag;

            {
                if (p.nextToken() != JsonToken.VALUE_STRING)
                    throw c.mappingException("Expected string (tag)");

                tag = p.readValueAs(String.class);
            }

            if (p.nextToken() != JsonToken.END_ARRAY)
                throw c.mappingException("Expected end of array");

            return new HasTagFilter(tag);
        }
    };

    public static FilterDeserializer<MatchKeyFilter> MATCH_KEY = new FilterDeserializer<MatchKeyFilter>() {
        @Override
        public MatchKeyFilter deserialize(JsonParser p, DeserializationContext c)
                throws IOException, JsonProcessingException {
            final String value;

            {
                if (p.nextToken() != JsonToken.VALUE_STRING)
                    throw c.mappingException("Expected string (key)");

                value = p.readValueAs(String.class);
            }

            if (p.nextToken() != JsonToken.END_ARRAY)
                throw c.mappingException("Expected end of array");

            return new MatchKeyFilter(value);
        }
    };

    public static FilterDeserializer<AndFilter> AND = new FilterDeserializer<AndFilter>() {
        @Override
        public AndFilter deserialize(JsonParser p, DeserializationContext c)
                throws IOException, JsonProcessingException {
            final List<Filter> statements = new ArrayList<>();

            while (p.nextToken() != JsonToken.END_ARRAY) {
                statements.add(p.readValueAs(Filter.class));
            }

            return new AndFilter(statements);
        }
    };

    public static FilterDeserializer<OrFilter> OR = new FilterDeserializer<OrFilter>() {
        @Override
        public OrFilter deserialize(JsonParser p, DeserializationContext c)
                throws IOException, JsonProcessingException {
            final List<Filter> statements = new ArrayList<>();

            while (p.nextToken() != JsonToken.END_ARRAY) {
                statements.add(p.readValueAs(Filter.class));
            }

            return new OrFilter(statements);
        }
    };

    public static class Deserializer extends JsonDeserializer<Filter> {
        private static final Map<String, FilterDeserializer<? extends Filter>> IMPL = new HashMap<>();

        static {
            IMPL.put(MatchTagFilter.OPERATOR, MATCH_TAG);
            IMPL.put(StartsWithFilter.OPERATOR, STARTS_WITH);
            IMPL.put(RegexFilter.OPERATOR, REGEX);
            IMPL.put(HasTagFilter.OPERATOR, HAS_TAG);
            IMPL.put(MatchKeyFilter.OPERATOR, MATCH_KEY);
            IMPL.put(AndFilter.OPERATOR, AND);
            IMPL.put(OrFilter.OPERATOR, OR);
        }

        @Override
        public Filter deserialize(JsonParser p, DeserializationContext c)
                throws IOException, JsonProcessingException {

            if (p.getCurrentToken() != JsonToken.START_ARRAY)
                throw c.mappingException("Expected start of array");

            final String operator;

            {
                if (p.nextToken() != JsonToken.VALUE_STRING)
                    throw c.mappingException("Expected string (operation)");

                operator = p.readValueAs(String.class);
            }

            final FilterDeserializer<? extends Filter> deserializer = IMPL
                    .get(operator);

            if (deserializer == null)
                throw c.mappingException("No such operator: " + operator);

            return deserializer.deserialize(p, c).optimize();
        }
    }

    public Filter optimize();
}
