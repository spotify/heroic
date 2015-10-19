package com.spotify.heroic.common;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.node.ValueNode;
import com.google.common.collect.ImmutableMap;

public class DurationSerialization {
    public static class Deserializer extends JsonDeserializer<Duration> {
        private static TimeUnit DEFAULT_UNIT = TimeUnit.MILLISECONDS;

        private static Pattern pattern = Pattern.compile("^(\\d+)([a-zA-Z]*)$");

        private static Map<String, TimeUnit> units = ImmutableMap.of("ms", TimeUnit.MILLISECONDS, "s",
                TimeUnit.SECONDS, "m", TimeUnit.MINUTES, "H", TimeUnit.HOURS);

        @Override
        public Duration deserialize(JsonParser p, DeserializationContext c) throws IOException, JsonProcessingException {
            /* fallback to default parser if object */
            if (p.getCurrentToken() == JsonToken.START_OBJECT) {
                return deserializeObject(p.readValueAsTree(), c);
            }

            if (p.getCurrentToken() == JsonToken.VALUE_STRING) {
                return deserializeString(p, c);
            }

            if (p.getCurrentToken().isNumeric()) {
                return deserializeLong(p);
            }

            throw c.mappingException("Cannot deserialize Duration from input");
        }

        private Duration deserializeLong(JsonParser p) throws IOException, JsonParseException {
            final long value = p.getLongValue();
            return new Duration(value, TimeUnit.MILLISECONDS);
        }

        private Duration deserializeObject(TreeNode tree, DeserializationContext c) throws JsonMappingException {
            if (tree == null) {
                throw c.mappingException("expected object");
            }

            TreeNode node;
            ValueNode valueNode;

            final long duration;
            final TimeUnit unit;

            if ((node = tree.get("duration")) != null && node.isValueNode()
                    && (valueNode = (ValueNode) node).isNumber()) {
                duration = valueNode.asLong();
            } else {
                throw c.mappingException("duration is not a numeric field");
            }

            if ((node = tree.get("unit")) != null && node.isValueNode()
                    && (valueNode = (ValueNode) node).isTextual()) {
                unit = TimeUnit.valueOf(valueNode.asText().toUpperCase());
            } else {
                unit = DEFAULT_UNIT;
            }

            return new Duration(duration, unit);
        }

        private Duration deserializeString(JsonParser p, DeserializationContext c) throws IOException {
            final String s = p.getValueAsString();
            final Matcher m = pattern.matcher(s);

            if (!m.matches()) {
                throw c.mappingException("not a valid duration: " + s);
            }

            final long duration = Long.valueOf(m.group(1));
            final String unitString = m.group(2);

            if (unitString.isEmpty()) {
                return new Duration(duration, DEFAULT_UNIT);
            }

            if ("w".equals(unitString)) {
                return new Duration(duration * 7, TimeUnit.DAYS);
            }

            final TimeUnit unit = units.get(unitString);

            if (unit == null) {
                throw c.mappingException("not a valid unit: " + unitString);
            }

            return new Duration(duration, unit);
        }
    }
}