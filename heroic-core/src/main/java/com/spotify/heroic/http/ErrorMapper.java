package com.spotify.heroic.http;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import lombok.extern.slf4j.Slf4j;

@Provider
@Slf4j
public class ErrorMapper implements ExceptionMapper<Error> {
    @Override
    public Response toResponse(Error e) {
        log.error("Fatal exception thrown in handler", e);
        System.exit(1);

        return null;
    }
}