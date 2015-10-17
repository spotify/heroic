package com.spotify.heroic;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.grammar.QueryParser;

import lombok.Data;

@Data
public class ExtraParameters {
    public static final ParameterSpecification CONFIGURE = ParameterSpecification.parameter("configure", "Automatically configure all backends.");

    private final Map<String, String> parameters;

    public boolean containsAny(String... keys) {
        return Arrays.stream(keys).anyMatch(parameters::containsKey);
    }

    public Optional<Filter> getFilter(String key, QueryParser parser) {
        final String value = parameters.get(key);

        if (value == null) {
            return Optional.empty();
        }

        return Optional.of(parser.parseFilter(value));
    }

    public Optional<Integer> getInteger(String key) {
        final String value = parameters.get(key);

        if (value == null) {
            return Optional.empty();
        }

        try {
            return Optional.of(Integer.parseInt(value));
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Key " + key + " exists, but does not contain a valid numeric value");
        }
    }

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

    public static ExtraParameters empty() {
        return new ExtraParameters(ImmutableMap.of());
    }

    public static ExtraParameters ofList(final List<String> input) {
        final ImmutableMap.Builder<String, String> result = ImmutableMap.builder();

        for (final String entry : input) {
            final int index = entry.indexOf('=');

            if (index < 0) {
                result.put(entry, "");
            } else {
                result.put(entry.substring(0, index), entry.substring(index + 1));
            }
        }

        return new ExtraParameters(result.build());
    }

    public String require(final String key) {
        return get(key).orElseThrow(() -> new IllegalStateException(key + ": is a required parameter"));
    }

    public boolean contains(ParameterSpecification parameter) {
        return contains(parameter.getName());
    }
}