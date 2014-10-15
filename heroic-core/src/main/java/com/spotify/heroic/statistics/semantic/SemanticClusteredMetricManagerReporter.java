package com.spotify.heroic.statistics.semantic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.statistics.ClusteredMetricManagerReporter;
import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

public class SemanticClusteredMetricManagerReporter implements ClusteredMetricManagerReporter {
    private static final String COMPONENT = "clustered-metric-manager";

    private final SemanticMetricRegistry registry;
    private final FutureReporter query;
    private final FutureReporter write;
    private final Map<Map<String, String>, FutureReporter> shardQueries = new HashMap<>();
    private final MetricId shardQueryBase;

    private static final Comparator<Map.Entry<String, String>> tagsComparator = new Comparator<Map.Entry<String, String>>() {
        @Override
        public int compare(Map.Entry<String, String> a, Map.Entry<String, String> b) {
            final int key = a.getKey().compareTo(b.getKey());

            if (key != 0)
                return key;

            return a.getValue().compareTo(b.getValue());
        }
    };

    public SemanticClusteredMetricManagerReporter(SemanticMetricRegistry registry) {
        this.registry = registry;
        final MetricId id = MetricId.build().tagged("component", COMPONENT);
        this.query = new SemanticFutureReporter(registry, id.tagged("what", "query", "unit", Units.READ));
        this.write = new SemanticFutureReporter(registry, id.tagged("what", "write", "unit", Units.WRITE));
        this.shardQueryBase = id.tagged("what", "shard-query", "unit", Units.READ);
    }

    @Override
    public FutureReporter.Context reportQuery() {
        return query.setup();
    }

    @Override
    public FutureReporter.Context reportWrite() {
        return write.setup();
    }

    @Override
    public synchronized FutureReporter.Context reportShardFullQuery(NodeMetadata metadata) {
        final FutureReporter r = shardQueries.get(metadata.getTags());

        if (r != null) {
            return r.setup();
        }

        final FutureReporter newReporter = new SemanticFutureReporter(registry, shardQueryBase.tagged("shard",
                formatShard(metadata.getTags())));
        shardQueries.put(metadata.getTags(), newReporter);
        return newReporter.setup();
    }

    private String formatShard(Map<String, String> tags) {
        final List<Map.Entry<String, String>> entries = new ArrayList<>(tags.entrySet());
        Collections.sort(entries, tagsComparator);
        final List<String> parts = new ArrayList<>(entries.size());

        for (final Map.Entry<String, String> e : entries) {
            parts.add(String.format("%s=%s", e.getKey(), e.getValue()));
        }

        return StringUtils.join(",", parts);
    }
}
