package com.spotify.heroic.statistics.noop;

import java.util.Map;
import java.util.Set;

import com.spotify.heroic.statistics.ClusteredMetadataManagerReporter;
import com.spotify.heroic.statistics.FutureReporter.Context;

public class NoopClusteredMetadataManagerReporter implements ClusteredMetadataManagerReporter {
    private NoopClusteredMetadataManagerReporter() {
    }

    @Override
    public Context reportFindTags() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportFindTagsShard(Map<String, String> shard) {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportFindKeys() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportFindKeysShard(Map<String, String> shard) {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportFindSeries() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportFindSeriesShard(Map<String, String> shard) {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportDeleteSeries() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportWrite() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportSearch() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportTagKeySuggest() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportTagSuggest() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportCount() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportKeySuggest() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportTagValuesSuggest() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public Context reportTagValueSuggest() {
        return NoopFutureReporterContext.get();
    }

    @Override
    public void registerShards(Set<Map<String, String>> knownShards) {
    }

    private static final NoopClusteredMetadataManagerReporter instance = new NoopClusteredMetadataManagerReporter();

    public static NoopClusteredMetadataManagerReporter get() {
        return instance;
    }
}
