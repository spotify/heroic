package com.spotify.heroic.yaml;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;

import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.cache.AggregationCacheBackend;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metrics.Backend;
import com.spotify.heroic.statistics.HeroicReporter;

@Data
public class HeroicConfigYAML {
	private ClusterManager.YAML cluster;
	private List<Backend.YAML> backends;
	private List<MetadataBackend.YAML> metadataBackends;
	private List<Consumer.YAML> consumers;
	private AggregationCacheBackend.YAML cache;
	private long maxAggregationMagnitude = HeroicConfig.MAX_AGGREGATION_MAGNITUDE;
	private long maxQueriableDataPoints = HeroicConfig.MAX_QUERIABLE_DATA_POINTS;
	private boolean updateMetadata = HeroicConfig.UPDATE_METADATA;
	private int port = HeroicConfig.DEFAULT_PORT;
	private String refreshClusterSchedule = HeroicConfig.DEFAULT_REFRESH_CLUSTER_SCHEDULE;
	private int groupLimit = HeroicConfig.DEFAULT_GROUP_LIMIT;
	private int groupLoadLimit = HeroicConfig.DEFAULT_GROUP_LOAD_LIMIT;

	private List<Backend> setupMetricBackends(String context,
			HeroicReporter reporter) throws ValidationException {
		final List<Backend> backends = new ArrayList<Backend>();

		int i = 0;

		for (final Backend.YAML backend : Utils.toList("backends",
				this.backends)) {
			final String c = context + "[" + i++ + "]";
			backends.add(backend.build(c, reporter.newMetricBackend(c)));
		}

		return backends;
	}

	private List<MetadataBackend> setupMetadataBackends(String context,
			HeroicReporter reporter) throws ValidationException {
		final List<MetadataBackend> backends = new ArrayList<MetadataBackend>();

		int i = 0;

		for (final MetadataBackend.YAML backend : Utils.toList(
				"metadataBackends", this.metadataBackends)) {
			final String c = context + "[" + i++ + "]";
			backends.add(backend.build(c, reporter.newMetadataBackend(c)));
		}

		return backends;
	}

	private List<Consumer> setupConsumers(String context,
			HeroicReporter reporter) throws ValidationException {

		final List<Consumer> consumers = new ArrayList<Consumer>();

		int i = 0;

		for (final Consumer.YAML consumer : Utils.toList("consumers",
				this.consumers)) {
			final String c = context + "[" + i++ + "]";
			consumers.add(consumer.build(c, reporter.newConsumerReporter(c)));
		}

		return consumers;
	}

	public HeroicConfig build(HeroicReporter reporter)
			throws ValidationException {
		final ClusterManager cluster;

		if (this.cluster != null) {
			cluster = this.cluster.build("cluster");
		} else {
			cluster = null;
		}

		final List<Backend> metricBackends = setupMetricBackends("backends",
				reporter);
		final List<MetadataBackend> metadataBackends = setupMetadataBackends(
				"metadataBackends", reporter);
		final List<Consumer> consumers = setupConsumers("consumers", reporter);

		final AggregationCache cache;

		if (this.cache == null) {
			cache = null;
		} else {
			final AggregationCacheBackend backend = this.cache.build("cache",
					reporter.newAggregationCacheBackend(null));
			cache = new AggregationCache(reporter.newAggregationCache(null),
					backend);
		}

		return new HeroicConfig(cluster, metricBackends, metadataBackends,
				consumers, cache, updateMetadata, port, refreshClusterSchedule,
				groupLimit, groupLoadLimit);
	}
}
