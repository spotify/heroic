package com.spotify.heroic.utils;

import java.util.concurrent.TimeUnit;

public class TimeUtils {
    public static TimeUnit parseUnitName(String unitName, TimeUnit defaultUnit) {
        if (unitName == null)
            return defaultUnit;

        final TimeUnit first = TimeUnit.valueOf(unitName.toUpperCase());

        if (first != null)
            return first;

        return defaultUnit;
    }

    public static long parseSize(Long inputSize, final TimeUnit unit, long defaultValue) {
        final long size;

        if (inputSize == null) {
            size = defaultValue;
        } else {
            size = TimeUnit.MILLISECONDS.convert(inputSize, unit);
        }

        if (size <= 0)
            throw new IllegalArgumentException("size must be a positive value");

        return size;
    }

    public static long parseExtent(Long inputExtent, final TimeUnit unit, final long size) {
        final long extent;

        if (inputExtent == null) {
            extent = size;
        } else {
            extent = TimeUnit.MILLISECONDS.convert(inputExtent, unit);
        }

        if (extent <= 0)
            throw new IllegalArgumentException("extent must be a positive value");

        return extent;
    }
}
