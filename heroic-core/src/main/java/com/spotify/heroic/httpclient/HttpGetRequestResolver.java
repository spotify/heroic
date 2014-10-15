package com.spotify.heroic.httpclient;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.async.Resolver;
import com.spotify.heroic.httpclient.exceptions.RpcNodeException;

@RequiredArgsConstructor
final class HttpGetRequestResolver<T> implements Resolver<T> {
    private final Class<T> bodyType;
    private final WebTarget target;

    @Override
    public T resolve() throws Exception {
        final Response response;

        try {
            response = target.request().get();
        } catch (final Exception e) {
            throw new RpcNodeException(target.getUri(), "request failed", e);
        }

        return HttpClientUtils.handleResponse(response, bodyType, target);
    }
}