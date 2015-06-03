package com.spotify.heroic.statistics;

import java.util.Map;

public interface ClusteredMetadataManagerReporter extends ClusteredManager {
    FutureReporter.Context reportFindTags();

    FutureReporter.Context reportFindTagsShard(Map<String, String> shard);

    FutureReporter.Context reportFindKeys();

    FutureReporter.Context reportFindKeysShard(Map<String, String> shard);

    FutureReporter.Context reportFindSeries();

    FutureReporter.Context reportFindSeriesShard(Map<String, String> shard);

    FutureReporter.Context reportDeleteSeries();

    FutureReporter.Context reportWrite();

    FutureReporter.Context reportSearch();

    FutureReporter.Context reportTagKeySuggest();

    FutureReporter.Context reportTagSuggest();

    FutureReporter.Context reportCount();

    FutureReporter.Context reportKeySuggest();

    FutureReporter.Context reportTagValuesSuggest();

    FutureReporter.Context reportTagValueSuggest();
}