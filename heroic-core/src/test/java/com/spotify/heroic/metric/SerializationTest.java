package com.spotify.heroic.metric;

import java.io.InputStream;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.HeroicPrimaryModule;

public class SerializationTest {
    private ObjectMapper mapper;

    @Before
    public void setup() {
        mapper = new ObjectMapper();
        mapper.registerModule(HeroicPrimaryModule.serializerModule());
    }

    @Test
    public void testResultGroups() throws Exception {
        try (InputStream in = openResource("result-groups.json")) {
            mapper.readValue(in, ResultGroups.class);
        }
    }

    private InputStream openResource(String path) {
        final Class<?> cls = SerializationTest.class;
        final String fullPath = cls.getPackage().getName().replace('.', '/') + "/" + path;
        return cls.getClassLoader().getResourceAsStream(fullPath);
    }
}