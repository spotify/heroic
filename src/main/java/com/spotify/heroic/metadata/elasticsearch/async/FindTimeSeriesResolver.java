package com.spotify.heroic.metadata.elasticsearch.async;

import java.util.HashSet;
import java.util.Set;

import lombok.RequiredArgsConstructor;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.search.SearchHit;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.metadata.elasticsearch.ElasticSearchMetadataBackend;
import com.spotify.heroic.metadata.model.FindTimeSeries;
import com.spotify.heroic.model.TimeSerie;

@RequiredArgsConstructor
public class FindTimeSeriesResolver implements
        Callback.Resolver<FindTimeSeries> {
    private final Client client;
    private final String index;
    private final String type;
    private final FilterBuilder filter;

    @Override
    public FindTimeSeries resolve() throws Exception {
        final Set<TimeSerie> timeSeries = new HashSet<TimeSerie>();

        final Iterable<SearchResponse> responses = new PaginatingSearchResponse(
                client, index, type, filter);

        for (final SearchResponse response : responses) {
            for (final SearchHit hit : response.getHits()) {
                timeSeries.add(ElasticSearchMetadataBackend.toTimeSerie(hit
                        .getSource()));
            }
        }

        return new FindTimeSeries(timeSeries, timeSeries.size());
    }
}
