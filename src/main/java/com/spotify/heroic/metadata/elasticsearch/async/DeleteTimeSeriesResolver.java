package com.spotify.heroic.metadata.elasticsearch.async;

import lombok.RequiredArgsConstructor;

import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.IndexDeleteByQueryResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.metadata.model.DeleteSeries;

@RequiredArgsConstructor
public class DeleteTimeSeriesResolver implements Callback.Resolver<DeleteSeries> {
    private final Client client;
    private final String index;
    private final String type;
    private final FilterBuilder filter;

    @Override
    public DeleteSeries resolve() throws Exception {
        final DeleteByQueryRequestBuilder request = client.prepareDeleteByQuery(index).setTypes(type);

        request.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter));

        final DeleteByQueryResponse response = request.execute().get();

        final IndexDeleteByQueryResponse result = response.getIndices().get(index);
        return new DeleteSeries(result.getSuccessfulShards(), result.getFailedShards());
    }
}
