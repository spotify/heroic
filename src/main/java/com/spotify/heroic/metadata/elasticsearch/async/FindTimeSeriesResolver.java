package com.spotify.heroic.metadata.elasticsearch.async;

import java.util.HashSet;
import java.util.Set;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.metadata.elasticsearch.ElasticSearchMetadataBackend;
import com.spotify.heroic.metadata.elasticsearch.FilterUtils;
import com.spotify.heroic.metadata.model.FindTimeSeries;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.filter.Filter;

@Slf4j
@RequiredArgsConstructor
public class FindTimeSeriesResolver implements
        Callback.Resolver<FindTimeSeries> {
    private final Client client;
    private final String index;
    private final String type;
    private final Filter filter;

    private static final int MAX_SIZE = 10000;

    @Override
    public FindTimeSeries resolve() throws Exception {
        final Set<Series> series = new HashSet<Series>();

        final SearchRequestBuilder request = client.prepareSearch(index)
                .setTypes(type).setSize(MAX_SIZE)
                .setScroll(TimeValue.timeValueSeconds(10))
                .setSearchType(SearchType.SCAN);

        if (filter != null)
            request.setQuery(QueryBuilders.filteredQuery(
                    QueryBuilders.matchAllQuery(),
                    FilterUtils.convertFilter(filter)));

        final SearchResponse response = request.get();
        final String scrollId = response.getScrollId();

        final String session = Integer.toHexString(new Object().hashCode());

        log.info("{}: Started scanning for time series (filter={})", session,
                filter.toString());

        int i = 0;

        while (true) {
            final SearchScrollRequestBuilder resp = client.prepareSearchScroll(
                    scrollId).setScroll(TimeValue.timeValueSeconds(10));

            final SearchResponse scroll = resp.get();

            boolean any = false;

            for (final SearchHit hit : scroll.getHits()) {
                any = true;
                i++;

                if (i % 100000 == 0)
                    log.info("{}: Got {} time series", session, i);

                series.add(ElasticSearchMetadataBackend.toTimeSerie(hit
                        .getSource()));
            }

            if (!any) {
                log.info("{}: Finished, loaded {} time series", session, i);
                break;
            }
        }

        return new FindTimeSeries(series, series.size());
    }
}
