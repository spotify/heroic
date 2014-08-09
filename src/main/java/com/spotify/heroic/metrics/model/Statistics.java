package com.spotify.heroic.metrics.model;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class Statistics {
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Builder {
        private Aggregator aggregator = Aggregator.EMPTY;
        private Row row = Row.EMPTY;
        private Cache cache = Cache.EMPTY;
        private Rpc rpc = Rpc.EMPTY;

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

        public Builder rpc(Rpc rpc) {
            this.rpc = rpc;
            return this;
        }

        public Statistics build() {
            return new Statistics(aggregator, row, cache, rpc);
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

        @JsonCreator
        public static Aggregator create(
                @JsonProperty(value = "sampleSize", required = true) Integer sampleSize,
                @JsonProperty(value = "outOfBounds", required = true) Integer outOfBounds,
                @JsonProperty(value = "uselessScan", required = true) Integer uselessScan) {
            return new Aggregator(sampleSize, outOfBounds, uselessScan);
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

        @JsonCreator
        public static Row create(
                @JsonProperty(value = "successful", required = true) Integer successful,
                @JsonProperty(value = "failed", required = true) Integer failed,
                @JsonProperty(value = "cancelled", required = true) Integer cancelled
                ) {
            return new Row(successful, failed, cancelled);
        }
    }

    @Data
    public static final class Cache {
        public static final Cache EMPTY = new Cache(0, 0, 0, 0);

        private final int hits;
        private final int conflicts;
        private final int cacheConflicts;
        private final int cachedNans;

        public Cache merge(Cache other) {
            return new Cache(this.hits + other.hits,
                    this.conflicts + other.conflicts, this.cacheConflicts + other.cacheConflicts,
                    this.cachedNans + other.cachedNans);
        }

        @JsonCreator
        public static Cache create(
                @JsonProperty(value = "hits", required = true) Integer hits,
                @JsonProperty(value = "conflicts", required = true) Integer conflicts,
                @JsonProperty(value = "cacheConflicts", required = true) Integer cacheConflicts,
                @JsonProperty(value = "cachedNans", required = true) Integer cachedNans
                ) {
            return new Cache(hits, conflicts, cacheConflicts, cachedNans);
        }
    }

    @Data
    public static final class Rpc {
        public static final Rpc EMPTY = new Rpc(0, 0, 0, 0);

        private final long successful;
        private final long failed;
        private final long onlineNodes;
        private final long offlineNodes;

        public Rpc merge(Rpc other) {
            return new Rpc(this.successful + other.successful, this.failed
                    + other.failed, this.onlineNodes + other.onlineNodes,
                    this.offlineNodes + other.offlineNodes);
        }

        @JsonCreator
        public static Rpc create(
                @JsonProperty(value = "successful", required = true) Long successful,
                @JsonProperty(value = "failed", required = true) Long failed,
                @JsonProperty(value = "onlineNodes", required = true) Long onlineNodes,
                @JsonProperty(value = "offlineNodes", required = true) Long offlineNodes) {
            return new Rpc(successful, failed, onlineNodes, offlineNodes);
        }
    }

    public static final Statistics EMPTY = new Statistics(Aggregator.EMPTY,
            Row.EMPTY, Cache.EMPTY, Rpc.EMPTY);

    private final Aggregator aggregator;
    private final Row row;
    private final Cache cache;
    private final Rpc rpc;

    public Statistics merge(Statistics other) {
        return new Statistics(aggregator.merge(other.aggregator),
                row.merge(other.row), cache.merge(other.cache),
                rpc.merge(other.rpc));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(final Statistics other) {
        return new Builder(other.aggregator, other.row, other.cache, other.rpc);
    }

    @JsonCreator
    public static Statistics create(
            @JsonProperty(value = "aggregator", required = true) Aggregator aggregator,
            @JsonProperty(value = "row", required = true) Row row,
            @JsonProperty(value = "cache", required = true) Cache cache,
            @JsonProperty(value = "rpc", required = true) Rpc rpc) {
        return new Statistics(aggregator, row, cache, rpc);
    }
}
