/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.http.render;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.util.Map;

import javax.imageio.ImageIO;
import javax.inject.Named;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.jfree.chart.JFreeChart;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.spotify.heroic.Query;
import com.spotify.heroic.QueryManager;
import com.spotify.heroic.metric.QueryResult;

@Path("render")
public class RenderResource {
    private static final int DEFAULT_WIDTH = 600;

    private static final int DEFAULT_HEIGHT = 400;

    @Inject
    @Named(MediaType.APPLICATION_JSON)
    private ObjectMapper mapper;

    @Inject
    private QueryManager query;

    @SuppressWarnings("unchecked")
    @GET
    @Path("image")
    @Produces("image/png")
    public Response render(@QueryParam("q") String queryString,
            @QueryParam("backend") String backendGroup, @QueryParam("title") String title,
            @QueryParam("width") Integer width, @QueryParam("height") Integer height,
            @QueryParam("highlight") String highlightRaw, @QueryParam("threshold") Double threshold)
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

        final Map<String, String> highlight;

        if (highlightRaw != null) {
            highlight = mapper.readValue(highlightRaw, Map.class);
        } else {
            highlight = null;
        }

        final Query q = query.newQueryFromString(queryString).build();

        final QueryResult result = this.query.useGroup(backendGroup).query(q).get();

        final JFreeChart chart =
                RenderUtils.createChart(result.getGroups(), title, highlight, threshold, height);

        final BufferedImage image = chart.createBufferedImage(width, height);

        final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        ImageIO.write(image, "png", buffer);

        return Response.ok(buffer.toByteArray()).build();
    }
}
