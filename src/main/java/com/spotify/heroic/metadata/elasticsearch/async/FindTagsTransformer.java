package com.spotify.heroic.metadata.elasticsearch.async;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import lombok.RequiredArgsConstructor;

import org.elasticsearch.client.Client;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.filter.AndFilter;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.MatchTagFilter;
import com.spotify.heroic.metadata.async.FindTagsReducer;
import com.spotify.heroic.metadata.elasticsearch.model.FindTagKeys;
import com.spotify.heroic.metadata.model.FindTags;

@RequiredArgsConstructor
public class FindTagsTransformer implements
        Callback.DeferredTransformer<FindTagKeys, FindTags> {
    private final Executor executor;
    private final Client client;
    private final String index;
    private final String type;
    private final Filter filter;

    @Override
    public Callback<FindTags> transform(FindTagKeys result) throws Exception {
        final List<Callback<FindTags>> callbacks = new ArrayList<Callback<FindTags>>();

        for (final String key : result.getKeys()) {
            callbacks.add(findSingle(key));
        }

        return ConcurrentCallback.newReduce(callbacks, new FindTagsReducer());
    }

    /**
     * Finds a single set of tags, excluding any criteria for this specific set
     * of tags.
     */
    private Callback<FindTags> findSingle(final String key) {
        final Filter filter = removeKeyFromFilter(this.filter, key);

        return ConcurrentCallback.newResolve(executor, new FindTagsResolver(
                client, index, type, filter, key));
    }

    private Filter removeKeyFromFilter(Filter filter, String key) {
        if (filter == null)
            return null;

        if (filter instanceof AndFilter) {
            final AndFilter and = (AndFilter) filter;

            final List<Filter> statements = new ArrayList<Filter>();

            for (final Filter f : and.getStatements()) {
                final Filter n = removeKeyFromFilter(f, key);

                if (n == null)
                    continue;

                statements.add(n);
            }

            if (statements.isEmpty())
                return null;

            return new AndFilter(statements).optimize();
        }

        if (filter instanceof MatchTagFilter) {
            final MatchTagFilter matchTag = (MatchTagFilter) filter;

            if (matchTag.getTag().equals(key))
                return null;

            return matchTag.optimize();
        }

        return filter;
    }
}
