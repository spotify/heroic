package com.spotify.heroic.metric.datastax.serializer;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class StringSerializer implements CustomSerializer<String> {
    private static final int IS_NULL = 0x0;
    private static final int IS_EMPTY = 0x1;
    private static final int IS_STRING = 0x2;

    private static final String EMPTY = "";

    private final Charset UTF_8 = Charset.forName("UTF-8");

    @Override
    public ByteBuffer serialize(String value) {
        if (value == null) {
            final ByteBuffer b = ByteBuffer.allocate(1).put((byte) IS_NULL);

            b.flip();
            return b;
        }

        if (value.isEmpty()) {
            final ByteBuffer b = ByteBuffer.allocate(1).put((byte) IS_EMPTY);

            b.flip();
            return b;
        }

        final byte[] bytes = value.getBytes(UTF_8);
        final ByteBuffer buffer = ByteBuffer.allocate(1 + bytes.length);
        buffer.put((byte) IS_STRING);
        buffer.put(bytes);

        buffer.flip();
        return buffer;
    }

    @Override
    public String deserialize(ByteBuffer buffer) {
        final byte flag = buffer.get();

        if (flag == IS_NULL)
            return null;

        if (flag == IS_EMPTY)
            return EMPTY;

        final byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, UTF_8);
    }
}
