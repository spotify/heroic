package com.spotify.heroic.http.rpc3;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.metrics.model.Statistics.Aggregator;
import com.spotify.heroic.metrics.model.Statistics.Cache;
import com.spotify.heroic.metrics.model.Statistics.Row;

@Data
@RequiredArgsConstructor
public class Rpc3Statistics {
    @Data
    public static final class Rpc {
        public static final Rpc EMPTY = new Rpc(0, 0, 0, 0, true);

        private final long successful;
        private final long failed;
        private final long onlineNodes;
        private final long offlineNodes;
        private final boolean clusterReady;

        public Rpc merge(Rpc other) {
            return new Rpc(this.successful + other.successful, this.failed
                    + other.failed, this.onlineNodes + other.onlineNodes,
                    this.offlineNodes + other.offlineNodes, this.clusterReady
                    && other.clusterReady);
        }

        @JsonCreator
        public static Rpc create(
                @JsonProperty(value = "successful", required = true) Long successful,
                @JsonProperty(value = "failed", required = true) Long failed,
                @JsonProperty(value = "onlineNodes", required = true) Long onlineNodes,
                @JsonProperty(value = "offlineNodes", required = true) Long offlineNodes,
                @JsonProperty(value = "clusterReady", required = true) Boolean clusterReady) {
            if (clusterReady == null)
                clusterReady = true;

            return new Rpc(successful, failed, onlineNodes, offlineNodes,
                    clusterReady);
        }
    }

    public static final Rpc3Statistics EMPTY = new Rpc3Statistics(
            Aggregator.EMPTY, Row.EMPTY, Cache.EMPTY, Rpc.EMPTY);

    private final Aggregator aggregator;
    private final Row row;
    private final Cache cache;
    private final Rpc rpc;

    public Rpc3Statistics merge(Rpc3Statistics other) {
        return new Rpc3Statistics(aggregator.merge(other.aggregator),
                row.merge(other.row), cache.merge(other.cache),
                rpc.merge(other.rpc));
    }

    @JsonCreator
    public static Rpc3Statistics create(
            @JsonProperty(value = "aggregator", required = true) Aggregator aggregator,
            @JsonProperty(value = "row", required = true) Row row,
            @JsonProperty(value = "cache", required = true) Cache cache,
            @JsonProperty(value = "rpc", required = true) Rpc rpc) {
        return new Rpc3Statistics(aggregator, row, cache, rpc);
    }
}
