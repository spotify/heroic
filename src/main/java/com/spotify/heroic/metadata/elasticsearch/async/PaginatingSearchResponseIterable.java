package com.spotify.heroic.metadata.elasticsearch.async;

import java.util.Iterator;

import lombok.RequiredArgsConstructor;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;

@RequiredArgsConstructor
public class PaginatingSearchResponseIterable implements
        Iterable<SearchResponse> {
    private final Client client;
    private final String index;
    private final String type;
    private final QueryBuilder query;

    @Override
    public Iterator<SearchResponse> iterator() {
        return new Iterator<SearchResponse>() {
            private SearchResponse next;
            private int from = 0;
            private final int size = 100;

            @Override
            public boolean hasNext() {
                final SearchRequestBuilder request = client
                        .prepareSearch(index).setTypes(type).setFrom(from)
                        .setSize(size);

                if (query != null) {
                    request.setQuery(query);
                }

                final SearchResponse next = request.get();

                if (next.getHits().getHits().length == 0)
                    return false;

                this.from += this.size;
                this.next = next;
                return true;
            }

            @Override
            public SearchResponse next() {
                final SearchResponse next = this.next;
                this.next = null;
                return next;
            }

            @Override
            public void remove() {
            };
        };
    }
}