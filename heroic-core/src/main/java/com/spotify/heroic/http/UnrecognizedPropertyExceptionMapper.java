package com.spotify.heroic.http;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;

@Provider
public class UnrecognizedPropertyExceptionMapper implements ExceptionMapper<UnrecognizedPropertyException> {
    @Override
    public Response toResponse(UnrecognizedPropertyException exception) {
        return Response.status(Response.Status.BAD_REQUEST).entity(new ErrorMessage(exception.getMessage()))
                .type(MediaType.APPLICATION_JSON).build();
    }
}