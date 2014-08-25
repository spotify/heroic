package com.spotify.heroic.http.error;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import com.spotify.heroic.http.model.MessageResponse;

import lombok.extern.slf4j.Slf4j;

@Provider
@Slf4j
public class CustomExceptionMapper implements ExceptionMapper<Exception> {
    @Override
    public Response toResponse(Exception exception) {
        log.error("Error in request", exception);
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity(new MessageResponse(exception.getMessage()))
                .type(MediaType.APPLICATION_JSON).build();
    }
}