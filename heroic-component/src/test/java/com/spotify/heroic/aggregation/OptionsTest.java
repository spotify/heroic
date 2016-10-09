package com.spotify.heroic.aggregation;

import com.spotify.heroic.common.Duration;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.spotify.heroic.test.LombokDataTest.verifyClassBuilder;

public class OptionsTest {
    final SamplingQuery s1 = new SamplingQuery("SECONDS", Duration.of(10, TimeUnit.MILLISECONDS),
        Duration.of(10, TimeUnit.MILLISECONDS));

    final SamplingQuery s2 = new SamplingQuery("SECONDS", Duration.of(20, TimeUnit.MILLISECONDS),
        Duration.of(20, TimeUnit.MILLISECONDS));

    @Test
    public void lombokDataTest() {
        verifyClassBuilder(Options.class).valueSupplier((type, secondary, name) -> {
            if ("sampling".equals(name)) {
                return Optional.of(Optional.of(secondary ? s1 : s2));
            }

            return Optional.empty();
        }).verify();
    }
}
