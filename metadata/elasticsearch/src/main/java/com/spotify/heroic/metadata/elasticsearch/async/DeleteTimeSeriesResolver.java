package com.spotify.heroic.metadata.elasticsearch.async;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.IndexDeleteByQueryResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import com.spotify.heroic.async.Resolver;
import com.spotify.heroic.metadata.model.DeleteSeries;

@Slf4j
@RequiredArgsConstructor
public class DeleteTimeSeriesResolver implements Resolver<DeleteSeries> {
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

        if (result.getFailedShards() > 0) {
            for (final ShardOperationFailedException failure : result.getFailures()) {
                log.error("Failed to delete series: {}", failure.toString());
            }

            throw new Exception("All series could not be deleted");
        }

        return new DeleteSeries(0);
    }
}
