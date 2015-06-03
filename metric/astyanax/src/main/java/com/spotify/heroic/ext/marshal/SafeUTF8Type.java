package com.spotify.heroic.ext.marshal;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.db.marshal.UTF8Type;

/**
 * An extension to the {@link UTF8Type} to allow for null and empty values.
 *
 * @author udoprog
 */
public class SafeUTF8Type extends AbstractType<String> {
    public static SafeUTF8Type instance = new SafeUTF8Type();

    private static final byte IS_NULL = 0x0;
    private static final byte IS_EMPTY_STRING = 0x1;
    private static final byte IS_STRING = 0x2;

    @Override
    public int compare(ByteBuffer o1, ByteBuffer o2) {
        return UTF8Type.instance.compare(o1, o2);
    }

    @Override
    public String compose(ByteBuffer bytes) {
        bytes = bytes.slice();
        byte flag = bytes.get();

        if (flag == IS_NULL)
            return null;

        if (flag == IS_EMPTY_STRING)
            return "";

        return UTF8Type.instance.compose(bytes.slice());
    }

    @Override
    public ByteBuffer decompose(String value) {
        if (value == null) {
            final ByteBuffer buffer = ByteBuffer.allocate(1).put(IS_NULL);
            buffer.rewind();
            return buffer;
        }

        if (value.isEmpty()) {
            final ByteBuffer buffer = ByteBuffer.allocate(1).put(IS_EMPTY_STRING);
            buffer.rewind();
            return buffer;
        }

        final ByteBuffer string = UTF8Type.instance.decompose(value);
        final ByteBuffer buffer = ByteBuffer.allocate(1 + string.capacity());
        buffer.put(IS_STRING).put(string);
        buffer.rewind();
        return buffer;
    }

    @Override
    public String getString(ByteBuffer bytes) {
        return compose(bytes);
    }

    @Override
    public ByteBuffer fromString(String source) throws MarshalException {
        return decompose(source);
    }

    @Override
    public void validate(ByteBuffer bytes) throws MarshalException {
        bytes = bytes.slice();
        final byte flag = bytes.get();

        if (flag == IS_NULL)
            return;

        if (flag == IS_EMPTY_STRING)
            return;

        UTF8Type.instance.validate(bytes.slice());
    }
}
