package com.spotify.heroic.aggregation.simple;

import com.spotify.heroic.aggregation.Empty;
import com.spotify.heroic.test.ValueSuppliers;

import java.lang.reflect.Type;
import java.util.Optional;

public class OfSupplier implements ValueSuppliers.ValueSupplier {
    @Override
    public Optional<Object> supply(
        final Type type, final boolean secondary, final String name
    ) {
        if ("of".equals(name)) {
            return Optional.of(secondary ? Optional.of(Empty.INSTANCE) : Optional.empty());
        }

        return Optional.empty();
    }
}
