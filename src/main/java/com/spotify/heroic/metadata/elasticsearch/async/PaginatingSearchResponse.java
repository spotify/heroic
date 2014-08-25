package com.spotify.heroic.metadata.elasticsearch.async;

import java.util.Iterator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

@Slf4j
@RequiredArgsConstructor
public class PaginatingSearchResponse implements Iterable<SearchResponse> {
    private final Client client;
    private final String index;
    private final String type;
    private final FilterBuilder filter;

    @Override
    public Iterator<SearchResponse> iterator() {
        return new Iterator<SearchResponse>() {
            private SearchResponse next;
            private int from = 0;
            private final int size = 100000;

            @Override
            public boolean hasNext() {
                final SearchRequestBuilder request = client
                        .prepareSearch(index).setTypes(type).setFrom(from)
                        .setSize(size);

                if (filter != null)
                    request.setQuery(QueryBuilders.filteredQuery(
                            QueryBuilders.matchAllQuery(), filter));

                final SearchResponse next = request.get();

                final SearchHit[] hits = next.getHits().getHits();

                if (hits.length == 0)
                    return false;

                log.info("Loaded SearchResponse {}-{}", from, from
                        + hits.length);

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