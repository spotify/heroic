package com.spotify.heroic;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.grammar.QueryParser;

import lombok.Data;

@Data
public class ExtraParameters {
    public static final ParameterSpecification CONFIGURE = ParameterSpecification.parameter("configure", "Automatically configure all backends.");

    private final Multimap<String, String> parameters;

    public boolean containsAny(String... keys) {
        return Arrays.stream(keys).anyMatch(parameters::containsKey);
    }

    public Optional<Filter> getFilter(String key, QueryParser parser) {
        final Collection<String> values = parameters.get(key);

        if (values.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(parser.parseFilter(values.iterator().next()));
    }

    public Optional<Integer> getInteger(String key) {
        final Collection<String> values = parameters.get(key);

        if (values.isEmpty()) {
            return Optional.empty();
        }

        try {
            return Optional.of(Integer.parseInt(values.iterator().next()));
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Key " + key + " exists, but does not contain a valid numeric value");
        }
    }

    public Optional<String> get(String key) {
        final Collection<String> values = parameters.get(key);

        if (values.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(values.iterator().next());
    }

    public boolean contains(String key) {
        return parameters.containsKey(key);
    }

    public static ExtraParameters empty() {
        return new ExtraParameters(ImmutableMultimap.of());
    }

    public static ExtraParameters ofList(final List<String> input) {
        final ImmutableMultimap.Builder<String, String> result = ImmutableMultimap.builder();

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

    public List<String> getAsList(final String key) {
        return ImmutableList.copyOf(parameters.get(key));
    }
}