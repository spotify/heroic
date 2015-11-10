package com.spotify.heroic.consumer.collectd;

import com.spotify.heroic.consumer.collectd.CollectdTypes.Field;

import lombok.Data;

public interface CollectdValue {
    public double convert(final CollectdTypes.Field field);

    @Data
    public static class Gauge implements CollectdValue {
        private final double value;

        @Override
        public double convert(final Field field) {
            return field.convertGauge(value);
        }
    }

    @Data
    public static class Absolute implements CollectdValue {
        private final long value;

        @Override
        public double convert(final Field field) {
            return field.convertAbsolute(value);
        }
    }

    @Data
    public static class Derive implements CollectdValue {
        private final long derivate;

        @Override
        public double convert(final Field field) {
            return field.convertAbsolute(derivate);
        }
    }

    @Data
    public static class Counter implements CollectdValue {
        private final long counter;

        @Override
        public double convert(final Field field) {
            return field.convertAbsolute(counter);
        }
    }
}
