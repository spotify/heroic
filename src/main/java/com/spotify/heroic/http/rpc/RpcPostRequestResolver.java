package com.spotify.heroic.http.rpc;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.http.general.ErrorMessage;

@RequiredArgsConstructor
public final class RpcPostRequestResolver<R, T> implements Callback.Resolver<T> {
    private final R request;
    private final Class<T> bodyType;
    private final WebTarget target;

    @Override
    public T resolve() throws Exception {
        final Response response;

        try {
            response = target.request().post(
                    Entity.entity(request, MediaType.APPLICATION_JSON));
        } catch (final Exception e) {
            throw new RpcNodeException(target.getUri(), "request failed", e);
        }

        final String contentType = response
                .getHeaderString(HttpHeaders.CONTENT_TYPE);

        if (contentType == null) {
            throw new RpcRemoteException(target.getUri(),
                    "No Content-Type in response");
        }

        if (!contentType.equals(MediaType.APPLICATION_JSON)) {
            throw new RpcRemoteException(target.getUri(),
                    "Got body of unexpected Content-Type: " + contentType);
        }

        if (response.getStatusInfo().getFamily() != Status.Family.SUCCESSFUL) {
            final ErrorMessage error = response.readEntity(ErrorMessage.class);
            throw new RpcRemoteException(target.getUri(), error.getMessage());
        }

        return response.readEntity(bodyType);
    }
}