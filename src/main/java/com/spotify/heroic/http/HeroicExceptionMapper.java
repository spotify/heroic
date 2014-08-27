package com.spotify.heroic.http;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import com.spotify.heroic.http.general.MessageResponse;

@Provider
public class HeroicExceptionMapper implements
ExceptionMapper<WebApplicationException> {
    @Override
    public Response toResponse(WebApplicationException exception) {
        return Response.status(exception.getResponse().getStatus())
                .entity(new MessageResponse(exception.getMessage()))
                .type(MediaType.APPLICATION_JSON).build();
    }
}