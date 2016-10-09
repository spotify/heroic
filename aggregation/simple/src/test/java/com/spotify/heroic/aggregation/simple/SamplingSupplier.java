package com.spotify.heroic.aggregation.simple;

import com.spotify.heroic.aggregation.SamplingQuery;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.test.ValueSuppliers;

import java.lang.reflect.Type;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class SamplingSupplier implements ValueSuppliers.ValueSupplier {
    final SamplingQuery s1 = new SamplingQuery("SECONDS", Duration.of(10, TimeUnit.MILLISECONDS),
        Duration.of(10, TimeUnit.MILLISECONDS));

    final SamplingQuery s2 = new SamplingQuery("SECONDS", Duration.of(20, TimeUnit.MILLISECONDS),
        Duration.of(20, TimeUnit.MILLISECONDS));

    @Override
    public Optional<Object> supply(
        final Type type, final boolean secondary, final String name
    ) {
        if ("sampling".equals(name)) {
            return Optional.of(Optional.of(secondary ? s1 : s2));
        }

        return Optional.empty();
    }
}
