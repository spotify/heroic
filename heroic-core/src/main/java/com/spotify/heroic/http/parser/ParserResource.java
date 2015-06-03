package com.spotify.heroic.http.parser;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.inject.Inject;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.grammar.QueryParser;

@Path("/parser")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ParserResource {
    @Inject
    private QueryParser parser;

    @GET
    @Path("/parse-filter")
    public Response parseQuery(@QueryParam("filter") String filter) {
        final Filter f = parser.parseFilter(filter);
        return Response.ok(f).build();
    }
}