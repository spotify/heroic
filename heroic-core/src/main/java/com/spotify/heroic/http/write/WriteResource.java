package com.spotify.heroic.http.write;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import lombok.Data;

import com.google.inject.Inject;
import com.spotify.heroic.ingestion.IngestionManager;
import com.spotify.heroic.metric.model.WriteMetric;

@Path("/write")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class WriteResource {
    @Inject
    private IngestionManager ingestion;

    @Data
    public static final class Message {
        private final String message;
    }

    @POST
    @Path("/metrics")
    public Response metrics(@QueryParam("backend") String backendGroup, WriteMetrics write) throws Exception {
        ingestion.write(backendGroup, new WriteMetric(write.getSeries(), write.getData()));
        return Response.status(Response.Status.OK).entity(new WriteMetricsResponse(true)).build();
    }
}
