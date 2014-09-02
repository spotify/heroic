package com.spotify.heroic.yaml;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;

import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.ClusterManagerImpl;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataBackendManager;
import com.spotify.heroic.metrics.MetricBackendManager;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.yaml.ConfigContext.Entry;

@Data
public class HeroicConfigYAML {
    private ClusterManagerImpl.YAML cluster = new ClusterManagerImpl.YAML();
    private MetricBackendManager.YAML metrics;
    private MetadataBackendManager.YAML metadata;
    private List<MetadataBackend.YAML> metadataBackends;
    private List<Consumer.YAML> consumers;
    private AggregationCache.YAML cache;
    private long maxAggregationMagnitude = HeroicConfig.MAX_AGGREGATION_MAGNITUDE;
    private long maxQueriableDataPoints = HeroicConfig.MAX_QUERIABLE_DATA_POINTS;
    private int port = HeroicConfig.DEFAULT_PORT;
    private String refreshClusterSchedule = HeroicConfig.DEFAULT_REFRESH_CLUSTER_SCHEDULE;

    private List<Consumer> setupConsumers(ConfigContext ctx,
            HeroicReporter reporter) throws ValidationException {

        final List<Consumer> consumers = new ArrayList<Consumer>();

        for (final Entry<Consumer.YAML> entry : ctx.iterate(this.consumers,
                "consumers")) {
            consumers.add(entry.getValue().build(entry.getContext(),
                    reporter.newConsumerReporter(entry.getContext())));
        }

        return consumers;
    }

    public HeroicConfig build(HeroicReporter reporter)
            throws ValidationException {

        final ConfigContext ctx = new ConfigContext();

        final MetricBackendManager metrics = this.metrics.build(
                ctx.extend("backend"), reporter.newMetricBackendManager());

        final ClusterManager cluster = ConfigUtils.notNull(
                ctx.extend("cluster"), this.cluster).build(
                        ctx.extend("cluster"), metrics);

        final AggregationCache cache;

        if (this.cache != null) {
            cache = this.cache.build(ctx.extend("cache"),
                    reporter.newAggregationCache(ctx.extend("cache")));
        } else {
            cache = AggregationCache.NULL;
        }

        final MetadataBackendManager metadata = this.metadata.build(
                ctx.extend("metadata"), reporter.newMetadataBackendManager());

        final List<Consumer> consumers = setupConsumers(
                ctx.extend("consumers"), reporter);

        return new HeroicConfig(cluster, metrics, metadata, consumers, cache,
                port, refreshClusterSchedule);
    }
}
