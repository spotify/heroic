package com.spotify.heroic.model;

import java.io.IOException;
import java.util.Map;

import lombok.RequiredArgsConstructor;
import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;

@RequiredArgsConstructor
public class SeriesSerializerImpl implements SeriesSerializer {
    private final Serializer<String> key;
    private final Serializer<Map<String, String>> tags;

    @Override
    public void serialize(SerialWriter buffer, Series value) throws IOException {
        this.key.serialize(buffer, value.getKey());
        this.tags.serialize(buffer, value.getTags());
    }

    @Override
    public Series deserialize(SerialReader buffer) throws IOException {
        final String key = this.key.deserialize(buffer);
        final Map<String, String> tags = this.tags.deserialize(buffer);
        return new Series(key, tags);
    }
}