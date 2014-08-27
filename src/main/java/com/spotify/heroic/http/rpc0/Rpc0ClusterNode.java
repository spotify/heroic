package com.spotify.heroic.http.rpc0;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import org.apache.commons.lang.NotImplementedException;
import org.glassfish.jersey.client.ClientConfig;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.metrics.model.MetricGroup;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.WriteMetric;
import com.spotify.heroic.model.WriteResult;

@Data
@ToString(of = "url")
public class Rpc0ClusterNode implements ClusterNode {
    private final String BASE = "rpc";

    private final URI url;
    private final ClientConfig config;
    private final Executor executor;

    @RequiredArgsConstructor
    private final class QueryResolver implements
    Callback.Resolver<MetricGroups> {
        private final Rpc0QueryBody request;
        private final Client client;

        @Override
        public MetricGroups resolve() throws Exception {
            final WebTarget target = client.target(url).path(BASE)
                    .path("query");
            final Rpc0MetricGroups source = target.request().post(
                    Entity.entity(request, MediaType.APPLICATION_JSON),
                    Rpc0MetricGroups.class);

            return convert(source);
        }

        private MetricGroups convert(Rpc0MetricGroups source) {
            return new MetricGroups(convert(source.getGroups()),
                    source.getStatistics());
        }

        private List<MetricGroup> convert(List<Rpc0MetricGroup> source) {
            final List<MetricGroup> groups = new ArrayList<MetricGroup>(
                    source.size());

            for (final Rpc0MetricGroup s : source) {
                groups.add(new MetricGroup(s.getTimeSerie(), s.getDatapoints()));
            }

            return groups;
        }
    }

    @Override
    public Callback<MetricGroups> query(final Series key,
            final Set<Series> series, final DateRange range,
            final AggregationGroup aggregationGroup) {
        final Rpc0QueryBody request = new Rpc0QueryBody(key, series, range,
                aggregationGroup);
        return ConcurrentCallback.newResolve(executor, new QueryResolver(
                request, ClientBuilder.newClient(config)));
    }

    @Override
    public Callback<WriteResult> write(List<WriteMetric> writes) {
        throw new NotImplementedException();
    }
}
