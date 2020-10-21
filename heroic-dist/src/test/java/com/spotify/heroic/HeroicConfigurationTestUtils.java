package com.spotify.heroic;

import java.io.InputStream;
import java.util.function.Supplier;

/**
 * Provides utility functions for testing the processing of heroic.yaml.
 */
public class HeroicConfigurationTestUtils {

    public static HeroicCoreInstance testConfiguration(final String name) throws Exception {
        final HeroicCore.Builder builder = HeroicCore.builder();
        builder.modules(HeroicModules.ALL_MODULES);
        builder.configStream(stream(name));
        return builder.build().newInstance();
    }

    public static Supplier<InputStream> stream(String name) {
        return () -> HeroicConfigurationTestUtils.class.getClassLoader().getResourceAsStream(name);
    }
}
