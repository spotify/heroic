package com.spotify.heroic.cluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.cluster.async.ClusterNodeLogHandle;
import com.spotify.heroic.cluster.async.NodeRegistryEntryReducer;
import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.yaml.Utils;
import com.spotify.heroic.yaml.ValidationException;

@Slf4j
@RequiredArgsConstructor
public class ClusterManagerImpl implements ClusterManager {
	@Data
	public static final class YAML {
		private ClusterDiscovery.YAML discovery;
		private Map<String, String> tags = new HashMap<String, String>();

		public ClusterManagerImpl build(String context)
				throws ValidationException {
			Utils.notNull(context + ".discovery", discovery);
			Utils.notEmpty(context + ".tags", tags);
			final ClusterDiscovery discovery = this.discovery.build(context
					+ ".discovery");
			return new ClusterManagerImpl(discovery, UUID.randomUUID(), tags);
		}
	}

	private final ClusterDiscovery discovery;

	@Getter
	private final UUID localNodeId;
	@Getter
	private final Map<String, String> localNodeTags;

	private final AtomicReference<NodeRegistry> registry = new AtomicReference<>();

	@Override
	public NodeRegistryEntry findNode(final Map<String, String> tags) {
		final NodeRegistry registry = this.registry.get();

		if (registry == null) {
			throw new IllegalStateException("Registry not ready");
		}

		return registry.findEntry(tags);
	}

	@Override
	public Callback<Void> refresh() {
		log.info("Cluster refresh in progress");
		final Callback<Collection<ClusterNode>> callback = discovery.getNodes();

		return callback
				.transform(new Callback.DeferredTransformer<Collection<ClusterNode>, Void>() {
					@Override
					public Callback<Void> transform(
							Collection<ClusterNode> result) throws Exception {
						final List<Callback<NodeRegistryEntry>> callbacks = new ArrayList<>();

						for (final ClusterNode clusterNode : result) {
							final Callback<NodeRegistryEntry> transform = clusterNode
									.getMetadata()
									.transform(
											new Callback.Transformer<NodeMetadata, NodeRegistryEntry>() {
												@Override
												public NodeRegistryEntry transform(
														NodeMetadata result)
																throws Exception {
													return new NodeRegistryEntry(
															clusterNode, result);
												}
											});
							transform.register(new ClusterNodeLogHandle(
									clusterNode));
							callbacks.add(transform);
						}
						return ConcurrentCallback.newReduce(
								callbacks,
								new NodeRegistryEntryReducer(registry, result
										.size()));
					}
				});
	}

	@Override
	public ClusterManager.Statistics getStatistics() {
		final NodeRegistry registry = this.registry.get();

		if (registry == null)
			return null;

		return new ClusterManager.Statistics(registry.getOnlineNodes(),
				registry.getOfflineNodes());
	}
}
