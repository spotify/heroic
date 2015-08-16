package com.spotify.heroic.common;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import lombok.RequiredArgsConstructor;

import org.apache.commons.lang3.tuple.Pair;

import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;

@RequiredArgsConstructor
public class TagsSerializer implements Serializer<Iterator<Entry<String, String>>> {
    private static final int NEXT = 0x01;
    private static final int END = 0x00;

    final Serializer<String> string;

    @Override
    public void serialize(SerialWriter buffer, Iterator<Map.Entry<String, String>> value) throws IOException {
        while (value.hasNext()) {
            buffer.write(NEXT);
            final Map.Entry<String, String> e = value.next();
            string.serialize(buffer, e.getKey());
            string.serialize(buffer, e.getValue());
        }

        buffer.write(END);
    }

    @Override
    public Iterator<Map.Entry<String, String>> deserialize(SerialReader buffer) throws IOException {
        return new Iterator<Map.Entry<String, String>>() {
            @Override
            public boolean hasNext() {
                try {
                    return buffer.read() == NEXT;
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to read buffer", e);
                }
            }

            @Override
            public Map.Entry<String, String> next() {
                final String key;
                final String value;

                try {
                    key = string.deserialize(buffer);
                    value = string.deserialize(buffer);
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to read buffer", e);
                }

                return Pair.of(key, value);
            }
        };
    }

    public static TagsSerializer construct(SerializerFramework s) {
        return new TagsSerializer(s.string());
    }
}