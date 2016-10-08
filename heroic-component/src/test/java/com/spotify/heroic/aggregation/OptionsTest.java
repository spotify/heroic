package com.spotify.heroic.aggregation;

import com.spotify.heroic.common.Duration;
import com.spotify.heroic.test.LombokDataTest;
import com.spotify.heroic.test.ValueSuppliers;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class OptionsTest {
    final SamplingQuery s1 = new SamplingQuery("SECONDS", Duration.of(10, TimeUnit.MILLISECONDS),
        Duration.of(10, TimeUnit.MILLISECONDS));

    final SamplingQuery s2 = new SamplingQuery("SECONDS", Duration.of(20, TimeUnit.MILLISECONDS),
        Duration.of(20, TimeUnit.MILLISECONDS));

    private final ValueSuppliers suppliers =
        ValueSuppliers.builder().supplier((type, secondary, name) -> {
            if ("sampling".equals(name)) {
                return Optional.of(Optional.of(secondary ? s1 : s2));
            }

            return Optional.empty();
        }).build();

    @Test
    public void lombokDataTest() {
        LombokDataTest.verifyClass(Options.class, suppliers);
    }
}
