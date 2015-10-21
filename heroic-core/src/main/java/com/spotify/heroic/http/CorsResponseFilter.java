package com.spotify.heroic.http;

import java.io.IOException;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.MultivaluedMap;

import lombok.Data;

@Data
public class CorsResponseFilter implements ContainerResponseFilter {
    private final String corsAllowOrigin;

    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
            throws IOException {
        final MultivaluedMap<String, Object> headers = responseContext.getHeaders();
        headers.add("Access-Control-Allow-Origin", corsAllowOrigin);
        headers.add("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");
        headers.add("Access-Control-Allow-Headers", "Content-Type");
    }
}