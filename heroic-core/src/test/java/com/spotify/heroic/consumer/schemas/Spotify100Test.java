package com.spotify.heroic.consumer.schemas;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.io.Resources;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Data;
import org.junit.Test;

public class Spotify100Test {
    private static ObjectMapper expectedObjectMapper() {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
        return mapper;
    }

    @Data
    public static class Expected {
        private final Optional<String> version;
        private final Optional<String> key;
        private final Optional<String> host;
        private final Optional<Long> time;
        private final Optional<Map<String, String>> attributes;
        private final Optional<Map<String, String>> resource;
        private final Optional<Double> value;
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

            expected.getKey().ifPresent(key -> {
                assertEquals(line + ": expected key", key, value.getKey());
            });

            expected.getHost().ifPresent(host -> {
                assertEquals(line + ": expected host", host, value.getHost());
            });

            expected.getTime().ifPresent(time -> {
                assertEquals(line + ": expected time", time, value.getTime());
            });

            expected.getAttributes().ifPresent(attributes -> {
                assertEquals(line + ": expected attributes", attributes, value.getAttributes());
            });

            expected.getResource().ifPresent(resource -> {
                assertEquals(line + ": expected resource", resource, value.getResource());
            });

            expected.getValue().ifPresent(v -> {
                assertEquals(line + ": expected value", v, value.getValue());
            });
        }
    }
}
