package com.spotify.heroic.metrics.model;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class Statistics {
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Builder {
        private Aggregator aggregator = Aggregator.EMPTY;
        private Row row = Row.EMPTY;
        private Cache cache = Cache.EMPTY;

        public Builder aggregator(Aggregator aggregator) {
            this.aggregator = aggregator;
            return this;
        }

        public Builder row(Row row) {
            this.row = row;
            return this;
        }

        public Builder cache(Cache cache) {
            this.cache = cache;
            return this;
        }

        public Statistics build() {
            return new Statistics(aggregator, row, cache);
        }
    }

    @Data
    public static final class Aggregator {
        public static final Aggregator EMPTY = new Aggregator(0, 0, 0);

        private final long sampleSize;
        private final long outOfBounds;

        /* aggregator performed a useless index scan. */
        private final long uselessScan;

        public Aggregator merge(Aggregator other) {
            return new Aggregator(this.sampleSize + other.sampleSize,
                    this.outOfBounds + other.outOfBounds, this.uselessScan
                            + other.uselessScan);
        }
    }

    @Data
    public static final class Row {
        public static final Row EMPTY = new Row(0, 0, 0);

        private final int successful;
        private final int failed;
        private final int cancelled;

        public Row merge(Row other) {
            return new Row(this.successful + other.successful, this.failed
                    + other.failed, this.cancelled + other.cancelled);
        }
    }

    @Data
    public static final class Cache {
        public static final Cache EMPTY = new Cache(0, 0, 0, 0, 0);

        private final int hits;
        private final int conflicts;
        private final int cacheConflicts;
        private final int cachedNans;
        private final int realNans;

        public Cache merge(Cache other) {
            return new Cache(this.hits + other.hits,
                this.conflicts + other.conflicts, this.cacheConflicts + other.cacheConflicts,
                this.cachedNans + other.cachedNans, this.realNans + other.realNans);
        }
    }

    public static final Statistics EMPTY = new Statistics(Aggregator.EMPTY,
            Row.EMPTY, Cache.EMPTY);

    private final Aggregator aggregator;
    private final Row row;
    private final Cache cache;

    public Statistics merge(Statistics other) {
        return new Statistics(aggregator.merge(other.aggregator),
                row.merge(other.row), cache.merge(other.cache));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(final Statistics other) {
        return new Builder(other.aggregator, other.row, other.cache);
    }
}