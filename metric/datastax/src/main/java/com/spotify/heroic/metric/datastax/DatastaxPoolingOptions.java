package com.spotify.heroic.metric.datastax;

import java.util.Optional;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Optional.empty;


public class DatastaxPoolingOptions {

    private final Optional<Integer> maxRequestsPerConnectionLocal;
    private final Optional<Integer> maxRequestsPerConnectionRemote;
    private final Optional<Integer> coreConnectionsPerHostLocal;
    private final Optional<Integer> coreConnectionsPerHostRemote;
    private final Optional<Integer> maxConnectionsPerHostLocal;
    private final Optional<Integer> maxConnectionsPerHostRemote;
    private final Optional<Integer> maxQueueSize;
    private final Optional<Integer> poolTimeoutMillis;
    private final Optional<Integer> idleTimeoutSeconds;
    private final Optional<Integer> heartbeatIntervalSeconds;

    public DatastaxPoolingOptions() {
        this(empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty(), empty());
    }

    @JsonCreator
    public DatastaxPoolingOptions(
        @JsonProperty("maxRequestsPerConnection.LOCAL") Optional<Integer> maxRequestsPerConnectionLocal,
        @JsonProperty("maxRequestsPerConnection.REMOTE") Optional<Integer> maxRequestsPerConnectionRemote,
        @JsonProperty("coreConnectionsPerHost.LOCAL") Optional<Integer> coreConnectionsPerHostLocal,
        @JsonProperty("coreConnectionsPerHost.REMOTE") Optional<Integer> coreConnectionsPerHostRemote,
        @JsonProperty("maxConnectionsPerHost.LOCAL") Optional<Integer> maxConnectionsPerHostLocal,
        @JsonProperty("maxConnectionsPerHost.REMOTE") Optional<Integer> maxConnectionsPerHostRemote,
        @JsonProperty("maxQueueSize") Optional<Integer> maxQueueSize,
        @JsonProperty("poolTimeoutMillis") Optional<Integer> poolTimeoutMillis,
        @JsonProperty("idleTimeoutSeconds") Optional<Integer> idleTimeoutSeconds,
        @JsonProperty("heartbeatIntervalSeconds") Optional<Integer> heartbeatIntervalSeconds
    ){
        this.maxRequestsPerConnectionLocal = maxRequestsPerConnectionLocal;
        this.maxRequestsPerConnectionRemote = maxRequestsPerConnectionRemote;
        this.coreConnectionsPerHostLocal = coreConnectionsPerHostLocal;
        this.coreConnectionsPerHostRemote = coreConnectionsPerHostRemote;
        this.maxConnectionsPerHostLocal = maxConnectionsPerHostLocal;
        this.maxConnectionsPerHostRemote = maxConnectionsPerHostRemote;
        this.maxQueueSize = maxQueueSize;
        this.poolTimeoutMillis = poolTimeoutMillis;
        this.idleTimeoutSeconds = idleTimeoutSeconds;
        this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
    }

    public void apply(final Builder builder) {
        final PoolingOptions pooling = new PoolingOptions();
        this.maxRequestsPerConnectionLocal.ifPresent(x -> pooling.setMaxConnectionsPerHost(HostDistance.LOCAL, x));
        this.maxRequestsPerConnectionRemote.ifPresent(x -> pooling.setMaxConnectionsPerHost(HostDistance.REMOTE, x));
        this.coreConnectionsPerHostLocal.ifPresent(x -> pooling.setCoreConnectionsPerHost(HostDistance.LOCAL, x));
        this.coreConnectionsPerHostRemote.ifPresent(x -> pooling.setCoreConnectionsPerHost(HostDistance.REMOTE, x));
        this.maxConnectionsPerHostLocal.ifPresent(x -> pooling.setMaxConnectionsPerHost(HostDistance.LOCAL, x));
        this.maxConnectionsPerHostRemote.ifPresent(x -> pooling.setMaxConnectionsPerHost(HostDistance.REMOTE, x));
        this.maxQueueSize.ifPresent(pooling::setMaxQueueSize);
        this.poolTimeoutMillis.ifPresent(pooling::setPoolTimeoutMillis);
        this.idleTimeoutSeconds.ifPresent(pooling::setIdleTimeoutSeconds);
        this.heartbeatIntervalSeconds.ifPresent(pooling::setHeartbeatIntervalSeconds);
        builder.withPoolingOptions(pooling);
    }


}
