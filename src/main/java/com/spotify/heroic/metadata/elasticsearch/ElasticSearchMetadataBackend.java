package com.spotify.heroic.metadata.elasticsearch;

import java.util.List;
import java.util.Set;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.FailedCallback;
import com.spotify.heroic.async.ResolvedCallback;
import com.spotify.heroic.backend.BackendException;
import com.spotify.heroic.injection.Startable;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.TimeSerieMatcher;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metadata.model.FindTimeSeries;
import com.spotify.heroic.statistics.MetadataBackendReporter;
import com.spotify.heroic.yaml.ValidationException;

@RequiredArgsConstructor
@Slf4j
public class ElasticSearchMetadataBackend implements MetadataBackend, Startable {
    public static class YAML implements MetadataBackend.YAML {
        public static String TYPE = "!elasticsearch-metadata";

        @Getter
        @Setter
        private List<String> seeds;

        @Getter
        @Setter
        private String clusterName = "elasticsearch";

        @Override
        public MetadataBackend build(String context,
                MetadataBackendReporter reporter) throws ValidationException {
            final String[] seeds = this.seeds.toArray(new String[this.seeds.size()]);
            return new ElasticSearchMetadataBackend(reporter, seeds, clusterName);
        }
    }

    private final MetadataBackendReporter reporter;
    private final String[] seeds;
    private final String clusterName;

    private Node node;

    @Override
    public void start() throws Exception {
        log.info("Starting");

        final Settings settings = ImmutableSettings.builder()
                .put("discovery.zen.ping.multicast.enabled", false)
                .putArray("discovery.zen.ping.unicast.hosts", seeds)
            .build();

        this.node = NodeBuilder.nodeBuilder().settings(settings).client(true).clusterName(clusterName).node();
    }

    @Override
    public Callback<FindTags> findTags(TimeSerieMatcher matcher,
            Set<String> include, Set<String> exclude) throws BackendException {
        return new FailedCallback<FindTags>(new Exception("not implemented yet"));
    }

    @Override
    public Callback<FindTimeSeries> findTimeSeries(TimeSerieMatcher matcher)
            throws BackendException {
        return new FailedCallback<FindTimeSeries>(new Exception("not implemented yet"));
    }

    @Override
    public Callback<FindKeys> findKeys(TimeSerieMatcher matcher)
            throws BackendException {
        return new FailedCallback<FindKeys>(new Exception("not implemented yet"));
    }

    @Override
    public Callback<Void> refresh() {
        return new ResolvedCallback<Void>(null);
    }

    @Override
    public boolean isReady() {
        return true;
    }
}
