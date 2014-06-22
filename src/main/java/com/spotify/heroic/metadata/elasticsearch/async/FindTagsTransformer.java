package com.spotify.heroic.metadata.elasticsearch.async;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import lombok.RequiredArgsConstructor;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.metadata.async.FindTagsReducer;
import com.spotify.heroic.metadata.elasticsearch.ElasticSearchMetadataBackend;
import com.spotify.heroic.metadata.elasticsearch.model.FindTagKeys;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metadata.model.TimeSerieQuery;

@RequiredArgsConstructor
public class FindTagsTransformer implements
        Callback.DeferredTransformer<FindTagKeys, FindTags> {
    private final Executor executor;
    private final Client client;
    private final String index;
    private final String type;
    private final TimeSerieQuery query;
    private final Set<String> includes;
    private final Set<String> excludes;

    @Override
    public Callback<FindTags> transform(FindTagKeys result) throws Exception {
        final List<Callback<FindTags>> callbacks = new ArrayList<Callback<FindTags>>();

        for (final String key : result.getKeys()) {
            if (includes != null && !includes.contains(key))
                continue;

            if (excludes != null && excludes.contains(key))
                continue;

            callbacks.add(findSingle(key));
        }

        return ConcurrentCallback.newReduce(callbacks, new FindTagsReducer());
    }

    /**
     * Finds a single set of tags, excluding any criteria for this specific set
     * of tags.
     * 
     * @param matcher
     * @param key
     * @return
     */
    private Callback<FindTags> findSingle(final String key) {
        final TimeSerieQuery newQuery;

        if (query.getMatchTags() == null) {
            newQuery = query;
        } else {
            final Map<String, String> newMatchTags = new HashMap<String, String>(
                    query.getMatchTags());
            newMatchTags.remove(key);
            newQuery = new TimeSerieQuery(query.getMatchKey(), newMatchTags,
                    query.getHasTags());
        }

        final QueryBuilder builder = ElasticSearchMetadataBackend
                .toQueryBuilder(newQuery);

        return ConcurrentCallback.newResolve(executor, new FindTagsResolver(
                client, index, type, builder, key));
    }
}
