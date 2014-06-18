package com.spotify.heroic.metrics.kairosdb;

import java.nio.ByteBuffer;

class DataPointColumnValue {
    public static final byte FLOAT_VALUE = 0x1;
    public static final byte DOUBLE_VALUE = 0x2;

    public static long toLong(ByteBuffer byteBuffer) {
        long ret = 0L;

        while (byteBuffer.hasRemaining()) {
            ret <<= 8;
            byte b = byteBuffer.get();
            ret |= (b & 0xFF);
        }

        return (ret);
    }

    public static double toDouble(ByteBuffer byteBuffer) {
        byte flag = byteBuffer.get();
        double ret = 0;

        if (flag == FLOAT_VALUE)
            ret = byteBuffer.getFloat();
        else
            ret = byteBuffer.getDouble();

        return ret;
    }
}