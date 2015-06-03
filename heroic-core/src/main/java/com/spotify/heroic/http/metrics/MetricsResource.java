package com.spotify.heroic.http.metrics;

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

import com.google.inject.Inject;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.model.BackendKey;
import com.spotify.heroic.utils.HttpAsyncUtils;

import eu.toolchain.async.AsyncFuture;

@Path("metrics")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MetricsResource {
    private final HttpAsyncUtils.Resume<List<BackendKey>, List<BackendKey>> KEYS = new HttpAsyncUtils.Resume<List<BackendKey>, List<BackendKey>>() {
        @Override
        public List<BackendKey> resume(List<BackendKey> result) throws Exception {
            return result;
        }
    };

    @Inject
    private HttpAsyncUtils httpAsync;

    @Inject
    private MetricManager metrics;

    @GET
    @Path("keys")
    public void metrics(@Suspended final AsyncResponse response, @QueryParam("group") String group,
            @QueryParam("limit") Integer limit) throws Exception {
        final AsyncFuture<List<BackendKey>> keys = metrics.useGroup(group).keys(null, null,
                limit == null ? 1000 : limit);
        httpAsync.handleAsyncResume(response, keys, KEYS);
    }
}