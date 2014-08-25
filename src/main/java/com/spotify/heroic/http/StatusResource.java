package com.spotify.heroic.http;

import java.util.Set;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.http.status.ServiceStatus;
import com.spotify.heroic.http.status.ServiceStatus.Cluster;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metrics.Backend;

@Path("/status")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class StatusResource {
    @Inject
    private Set<Consumer> consumers;

    @Inject
    private Set<Backend> backends;

    @Inject
    private Set<MetadataBackend> metadataBackends;

    @Inject
    private ClusterManager cluster;

    @GET
    public Response get() {
        final ServiceStatus.Consumer consumers = buildConsumerStatus();
        final ServiceStatus.Backend backends = buildBackendStatus();
        final ServiceStatus.MetadataBackend metadataBackends = buildMetadataBackendStatus();

        final ServiceStatus.Cluster cluster = buildClusterStatus();

        final boolean allOk = consumers.isOk() && backends.isOk()
                && metadataBackends.isOk() && cluster.isOk();

        final ServiceStatus response = new ServiceStatus(allOk, consumers,
                backends, metadataBackends, cluster);

        return Response.status(Response.Status.OK).entity(response).build();
    }

    private ServiceStatus.Cluster buildClusterStatus() {
        if (cluster == ClusterManager.NULL)
            return new ServiceStatus.Cluster(true, 0, 0);

        final ClusterManager.Statistics s = cluster.getStatistics();

        if (s == null)
            return new ServiceStatus.Cluster(true, 0, 0);

        return new ServiceStatus.Cluster(s.getOfflineNodes() == 0,
                s.getOnlineNodes(), s.getOfflineNodes());
    }

    private ServiceStatus.Backend buildBackendStatus() {
        final int available = backends.size();

        int ready = 0;

        for (final Backend backend : backends) {
            if (backend.isReady())
                ready += 1;
        }

        return new ServiceStatus.Backend(available == ready, available, ready);
    }

    private ServiceStatus.Consumer buildConsumerStatus() {
        final int available = consumers.size();

        int ready = 0;

        for (final Consumer consumer : consumers) {
            if (consumer.isReady())
                ready += 1;
        }

        return new ServiceStatus.Consumer(available == ready, available, ready);
    }

    private ServiceStatus.MetadataBackend buildMetadataBackendStatus() {
        final int available = metadataBackends.size();

        int ready = 0;

        for (final MetadataBackend metadata : metadataBackends) {
            if (metadata.isReady())
                ready += 1;
        }

        return new ServiceStatus.MetadataBackend(available == ready, available,
                ready);
    }
}
