package com.spotify.heroic.http;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import com.spotify.heroic.grammar.ParseException;

@Provider
public class ParseExceptionMapper implements ExceptionMapper<ParseException> {
    @Override
    public Response toResponse(ParseException e) {
        final ParseErrorMessage entity = new ParseErrorMessage(e.getMessage(), e.getLine(), e.getCol(), e.getLineEnd(),
                e.getColEnd());
        return Response.status(Response.Status.BAD_REQUEST).entity(entity).type(MediaType.APPLICATION_JSON).build();
    }
}