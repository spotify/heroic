package com.spotify.heroic.http.write;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import lombok.Data;

import com.spotify.heroic.metric.ClusteredMetricManager;
import com.spotify.heroic.metric.MetricFormatException;
import com.spotify.heroic.metric.model.WriteMetric;

@Path("/write")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class WriteResource {
    @Inject
    private ClusteredMetricManager metrics;

    @Data
    public static final class Message {
        private final String message;
    }

    @POST
    @Path("/metrics")
    public Response metrics(@QueryParam("backend") String backendGroup,
            WriteMetrics write) throws Exception {
        try {
            metrics.bufferWrite(backendGroup, new WriteMetric(
                    write.getSeries(), write.getData()));
        } catch (final MetricFormatException e) {
            throw new Exception("Invalid write: " + e.getMessage());
        }

        return Response.status(Response.Status.OK)
                .entity(new WriteMetricsResponse(true)).build();
    }
}
