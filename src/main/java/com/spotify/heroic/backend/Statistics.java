package com.spotify.heroic.backend;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@RequiredArgsConstructor
public class Statistics {
    @RequiredArgsConstructor
    public static final class Aggregator {
        public static final Aggregator EMPTY = new Aggregator(0, 0);

        @Getter
        private final long sampleSize;

        @Getter
        private final long outOfBounds;

        public Aggregator merge(Aggregator other) {
            return new Aggregator(this.sampleSize + other.sampleSize, this.outOfBounds + other.outOfBounds);
        }
    }

    public static final class Row {
        public static final Row EMPTY = new Row(0, 0, 0);

        @Getter
        private final int total;

        @Getter
        private final int failed;

        @Getter
        private final int successful;

        @Getter
        private final int cancelled;

        public Row(int successful, int failed, int cancelled) {
            this.total = successful + failed + cancelled;
            this.successful = successful;
            this.failed = failed;
            this.cancelled = cancelled;
        }

        public Row merge(Row other) {
            return new Row(this.successful + other.successful, this.failed + other.failed, this.cancelled + other.cancelled);
        }
    }

    @RequiredArgsConstructor
    public static final class Cache {
        public static final Cache EMPTY = new Cache(0, 0, 0);

        @Getter
        private final int hits;

        @Getter
        private final int conflicts;

        @Getter
        private final int duplicates;

        public Cache merge(Cache other) {
            return new Cache(this.hits + other.hits, this.conflicts + other.conflicts, this.duplicates + other.duplicates);
        }
    }

    @Getter
    @Setter
    private Aggregator aggregator = Aggregator.EMPTY;

    @Getter
    @Setter
    private Row row = Row.EMPTY;

    @Getter
    @Setter
    private Cache cache = Cache.EMPTY;

    public Statistics merge(Statistics other) {
        final Statistics s = new Statistics();
        s.setAggregator(aggregator.merge(other.aggregator));
        s.setRow(row.merge(other.row));
        s.setCache(cache.merge(other.cache));
        return s;
    }
}