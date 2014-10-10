package com.spotify.heroic.metadata.elasticsearch.async;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import lombok.RequiredArgsConstructor;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;

import com.spotify.heroic.async.DeferredTransformer;
import com.spotify.heroic.async.Future;
import com.spotify.heroic.async.Futures;
import com.spotify.heroic.async.ResolvedFuture;
import com.spotify.heroic.filter.AndFilter;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.MatchTagFilter;
import com.spotify.heroic.filter.TrueFilter;
import com.spotify.heroic.metadata.elasticsearch.ElasticSearchUtils;
import com.spotify.heroic.metadata.model.FindTagKeys;
import com.spotify.heroic.metadata.model.FindTags;

@RequiredArgsConstructor
public class FindTagsTransformer implements DeferredTransformer<FindTagKeys, FindTags> {
    private final Executor executor;
    private final Client client;
    private final String index;
    private final String type;
    private final Filter filter;

    @Override
    public Future<FindTags> transform(FindTagKeys result) throws Exception {
        final List<Future<FindTags>> callbacks = new ArrayList<Future<FindTags>>();

        for (final String key : result.getKeys()) {
            callbacks.add(findSingle(key));
        }

        return Futures.reduce(callbacks, FindTags.reduce());
    }

    /**
     * Finds a single set of tags, excluding any criteria for this specific set of tags.
     */
    private Future<FindTags> findSingle(final String key) {
        final Filter filter = removeKeyFromFilter(this.filter, key);

        final FilterBuilder f = ElasticSearchUtils.convertFilter(filter);

        if (f == null)
            return new ResolvedFuture<FindTags>(FindTags.EMPTY);

        return Futures.resolve(executor, new FindTagsResolver(client, index, type, f, key));
    }

    private Filter removeKeyFromFilter(Filter filter, String key) {
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
                return TrueFilter.get();

            return new AndFilter(statements).optimize();
        }

        if (filter instanceof MatchTagFilter) {
            final MatchTagFilter matchTag = (MatchTagFilter) filter;

            if (matchTag.getTag().equals(key))
                return TrueFilter.get();

            return matchTag.optimize();
        }

        return filter;
    }
}
