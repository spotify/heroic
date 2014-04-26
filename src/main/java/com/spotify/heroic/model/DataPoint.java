package com.spotify.heroic.model;

import java.nio.ByteBuffer;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import com.netflix.astyanax.model.Column;
import com.spotify.heroic.backend.kairosdb.DataPointsRowKey;

@ToString(of = { "timestamp", "value" })
@EqualsAndHashCode(of = { "timestamp", "value" })
public class DataPoint implements Comparable<DataPoint> {
    public static class Name {
        private static final int LONG_FLAG = 0x0;
        private static final int FLOAT_FLAG = 0x1;

        public static long toTimeStamp(long base, int name) {
            long offset = name >>> 1;
            return base + offset;
        }

        public static boolean isLong(int name) {
            return (name & 0x1) == LONG_FLAG;
        }

        public static long toStartTimeStamp(final long start,
                final long timestamp) {
            if (start < timestamp)
                return getColumnName(timestamp, timestamp, true);

            return getColumnName(timestamp, start, true);
        }

        public static long toEndTimeStamp(final long end, final long timestamp) {
            if (end > (timestamp + DataPointsRowKey.MAX_WIDTH))
                return getColumnName(timestamp, timestamp
                        + DataPointsRowKey.MAX_WIDTH, false);

            return getColumnName(timestamp, end, false);
        }

        private static long getColumnName(long rowTime, long timestamp,
                boolean isInteger) {
            final long offset = timestamp - rowTime;

            if (offset > DataPointsRowKey.MAX_WIDTH) {
                throw new RuntimeException("Offset exceeds max width: "
                        + offset);
            }

            if (isInteger) {
                return offset << 1 | LONG_FLAG;
            } else {
                return offset << 1 | FLOAT_FLAG;
            }
        }
    }

    public static class Value {
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

    public static DataPoint fromColumn(DataPointsRowKey rowKey,
            Column<Integer> column) {
        final int name = column.getName();
        final long timestamp = Name.toTimeStamp(rowKey.getTimestamp(), name);
        final ByteBuffer bytes = column.getByteBufferValue();

        if (Name.isLong(name))
            return new DataPoint(timestamp, Value.toLong(bytes));

        return new DataPoint(timestamp, Value.toDouble(bytes));
    }

    @Getter
    private final long timestamp;

    @Getter
    private final double value;

    public DataPoint(long timestamp, double value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public DataPoint(long timestamp, long value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    @Override
    public int compareTo(DataPoint o) {
        return Long.compare(timestamp, o.timestamp);
    }
}
