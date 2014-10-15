package com.spotify.heroic.http.render;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;

import javax.imageio.ImageIO;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import lombok.extern.slf4j.Slf4j;

import org.jfree.chart.JFreeChart;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.async.Future;
import com.spotify.heroic.http.query.QueryMetrics;
import com.spotify.heroic.http.query.QueryPrepared;
import com.spotify.heroic.metric.ClusteredMetricManager;
import com.spotify.heroic.metric.model.QueryMetricsResult;

@Slf4j
@Path("render")
public class RenderResource {
    private static final int DEFAULT_WIDTH = 600;

    private static final int DEFAULT_HEIGHT = 400;

    @Inject
    @Named(MediaType.APPLICATION_JSON)
    private ObjectMapper mapper;

    @Inject
    private ClusteredMetricManager metrics;

    @GET
    @Path("image")
    @Produces("image/png")
    public Response render(@QueryParam("query") String query, @QueryParam("backend") String backendGroup,
            @QueryParam("title") String title, @QueryParam("width") Integer width, @QueryParam("height") Integer height)
                    throws Exception {
        if (query == null) {
            throw new BadRequestException("'query' must be defined");
        }

        if (width == null) {
            width = DEFAULT_WIDTH;
        }

        if (height == null) {
            height = DEFAULT_HEIGHT;
        }

        final QueryMetrics queryMetrics = mapper.readValue(query, QueryMetrics.class);
        final QueryPrepared q = QueryPrepared.create(backendGroup, queryMetrics);

        log.info("Render: {}", q);

        final Future<QueryMetricsResult> callback = metrics.query(q.getBackendGroup(), q.getFilter(), q.getGroupBy(),
                q.getRange(), q.getAggregation());

        final QueryMetricsResult result = callback.get();

        final String graphTitle = title == null ? "Heroic Graph" : title;

        final JFreeChart chart = RenderUtils.createChart(result.getMetricGroups(), graphTitle);

        final BufferedImage image = chart.createBufferedImage(width, height);

        final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        ImageIO.write(image, "png", buffer);

        return Response.ok(buffer.toByteArray()).build();
    }
}
