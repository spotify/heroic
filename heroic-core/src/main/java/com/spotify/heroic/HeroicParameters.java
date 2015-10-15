package com.spotify.heroic;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;

import lombok.Data;

@Data
public class HeroicParameters {
    private final Map<String, String> parameters;

    public Optional<String> get(String key) {
        final String value = parameters.get(key);

        if (value == null) {
            return Optional.empty();
        }

        return Optional.of(value);
    }

    public boolean contains(String key) {
        return parameters.containsKey(key);
    }

    public static HeroicParameters empty() {
        return new HeroicParameters(ImmutableMap.of());
    }

    public static HeroicParameters ofList(final List<String> input) {
        final ImmutableMap.Builder<String, String> result = ImmutableMap.builder();

        for (final String entry : input) {
            final int index = entry.indexOf('=');

            if (index < 0) {
                result.put(entry, "");
            } else {
                result.put(entry.substring(0, index), entry.substring(index + 1));
            }
        }

        return new HeroicParameters(result.build());
    }
}