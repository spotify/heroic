package com.spotify.heroic.consumer.schemas;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.auto.value.AutoValue;
import com.google.common.io.Resources;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public class Spotify100Test {
    private static ObjectMapper expectedObjectMapper() {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
        return mapper;
    }

    @AutoValue
    abstract static class Expected {
        @JsonCreator
        static Expected create(
            @JsonProperty("version") Optional<String> version,
            @JsonProperty("key") Optional<String> key,
            @JsonProperty("host") Optional<String> host,
            @JsonProperty("time") Optional<Long> time,
            @JsonProperty("attributes") Optional<Map<String, String>> attributes,
            @JsonProperty("resource") Optional<Map<String, String>> resource,
            @JsonProperty("value") Optional<Double> value
        ) {
            return new AutoValue_Spotify100Test_Expected(version, key, host, time, attributes, resource, value);
        }

        abstract Optional<String> version();
        abstract Optional<String> key();
        abstract Optional<String> host();
        abstract Optional<Long> time();
        abstract Optional<Map<String, String>> attributes();
        abstract Optional<Map<String, String>> resource();
        abstract Optional<Double> value();
    }

    @Test
    public void testCases() throws Exception {
        final ObjectMapper expectedMapper = expectedObjectMapper();
        final ObjectMapper mapper = Spotify100.objectMapper();

        final List<String> lines = Resources.readLines(
            Resources.getResource(Spotify100Test.class, "spotify-100-tests.txt"),
            StandardCharsets.UTF_8);

        int i = 0;

        for (final String test : lines) {
            if (test.trim().isEmpty()) {
                continue;
            }

            if (test.trim().startsWith("#")) {
                continue;
            }

            final String[] parts = test.split("\\|");

            final int line = i++;

            final Spotify100.JsonMetric value;

            try {
                value = mapper.readValue(parts[0].trim(), Spotify100.JsonMetric.class);
            } catch (final Exception e) {
                throw new RuntimeException(line + ": " + e.getMessage(), e);
            }

            final Expected expected = expectedMapper.readValue(parts[1].trim(), Expected.class);

            expected.key().ifPresent(key -> {
                assertEquals(line + ": expected key", key, value.getKey());
            });

            expected.host().ifPresent(host -> {
                assertEquals(line + ": expected host", host, value.getHost());
            });

            expected.time().ifPresent(time -> {
                assertEquals(line + ": expected time", time, value.getTime());
            });

            expected.attributes().ifPresent(attributes -> {
                assertEquals(line + ": expected attributes", attributes, value.getAttributes());
            });

            expected.resource().ifPresent(resource -> {
                assertEquals(line + ": expected resource", resource, value.getResource());
            });

            expected.value().ifPresent(v -> {
                assertEquals(line + ": expected value", v, value.getValue());
            });
        }
    }
}
