package com.spotify.heroic.metrics.kairosdb;


class DataPointColumnKey {
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