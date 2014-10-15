package com.spotify.heroic.http;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.httpclient.model.ErrorMessage;

@Slf4j
@Provider
public class HeroicExceptionMapper implements ExceptionMapper<WebApplicationException> {
    @Override
    public Response toResponse(WebApplicationException exception) {
        log.error("error in request", exception);
        return Response.status(exception.getResponse().getStatus()).entity(new ErrorMessage(exception.getMessage()))
                .type(MediaType.APPLICATION_JSON).build();
    }
}