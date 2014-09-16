package com.spotify.heroic.http.cluster;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;

@Path("/cluster")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class ClusterResource {
    @Inject
    private ClusterManager cluster;

    /**
     * Encode/Decode functions, helpful when interacting with cassandra through
     * cqlsh.
     */
    @GET
    @Path("/status")
    public Response decodeRowKey() {
        final List<ClusterNodeStatus> nodes = convert(cluster.getNodes());
        final ClusterStatus status = new ClusterStatus(nodes,
                cluster.getStatistics());
        return Response.status(Response.Status.OK).entity(status).build();
    }

    private List<ClusterNodeStatus> convert(List<NodeRegistryEntry> nodes) {
        final List<ClusterNodeStatus> result = new ArrayList<>();

        for (final NodeRegistryEntry e : nodes)
            result.add(convert(e));

        return result;
    }

    private ClusterNodeStatus convert(NodeRegistryEntry e) {
        final NodeMetadata m = e.getMetadata();

        return new ClusterNodeStatus(e.getClusterNode().getClass(), e.getUri(),
                m.getId(), m.getVersion(), m.getTags(), m.getCapabilities());
    }
}
