package com.spotify.heroic;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.InputStream;

import static org.junit.Assert.assertFalse;

public class HeroicConfigTest {
    private final ObjectMapper mapper = HeroicMappers.config();

    public InputStream resource(final String name) {
        return getClass()
            .getClassLoader()
            .getResourceAsStream(getClass().getPackage().getName().replace('.', '/') + '/' + name);
    }

    @Test
    public void testConfigEmpty() throws Exception {
        try (final InputStream in = resource("Config.Empty.yml")) {
            assertFalse(HeroicConfig.loadConfig(mapper, in).isPresent());
        }
    }
}
