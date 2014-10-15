package com.spotify.heroic.httpclient;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.spotify.heroic.httpclient.exceptions.RpcRemoteException;
import com.spotify.heroic.httpclient.model.ErrorMessage;

public class HttpClientUtils {

    public static <T> T handleResponse(Response response, Class<T> bodyType, WebTarget target)
            throws RpcRemoteException {
        final String contentType = response.getHeaderString(HttpHeaders.CONTENT_TYPE);

        if (contentType == null) {
            throw new RpcRemoteException(target.getUri(), "No Content-Type in response",
                    response.readEntity(String.class));
        }

        if (response.getStatusInfo().getFamily() != Status.Family.SUCCESSFUL) {
            if (contentType.equals(MediaType.APPLICATION_JSON)) {
                final ErrorMessage error = response.readEntity(ErrorMessage.class);
                throw new RpcRemoteException(target.getUri(), error.getMessage());
            }

            final String entity = response.readEntity(String.class);
            throw new RpcRemoteException(target.getUri(), "Error from request", entity);
        }

        if (!contentType.equals(MediaType.APPLICATION_JSON)) {
            throw new RpcRemoteException(target.getUri(), "Got body of unexpected Content-Type: " + contentType,
                    response.readEntity(String.class));
        }

        return response.readEntity(bodyType);
    }
}
