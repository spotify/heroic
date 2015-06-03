package com.spotify.heroic.model;

import java.io.IOException;

import lombok.RequiredArgsConstructor;
import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;

@RequiredArgsConstructor
public class SamplingSerializerImpl implements SamplingSerializer {
    private final Serializer<Long> longS;

    @Override
    public void serialize(SerialWriter buffer, Sampling value) throws IOException {
        longS.serialize(buffer, value.getSize());
        longS.serialize(buffer, value.getExtent());
    }

    @Override
    public Sampling deserialize(SerialReader buffer) throws IOException {
        final long size = longS.deserialize(buffer);
        final long extent = longS.deserialize(buffer);
        return new Sampling(size, extent);
    }
}