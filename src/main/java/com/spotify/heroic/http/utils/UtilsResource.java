package com.spotify.heroic.http.utils;

import java.nio.ByteBuffer;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.io.BaseEncoding;
import com.netflix.astyanax.Serializer;
import com.spotify.heroic.aggregationcache.cassandra.model.CacheKey;
import com.spotify.heroic.aggregationcache.cassandra.model.CacheKeySerializer;
import com.spotify.heroic.metric.heroic.MetricsRowKey;
import com.spotify.heroic.metric.heroic.MetricsRowKeySerializer;

@Path("/utils")
public class UtilsResource {
    /**
     * Encode/Decode functions, helpful when interacting with cassandra through cqlsh.
     */
    @POST
    @Path("/decode-row-key")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    public Response decodeRowKey(final String data) {
        final MetricsRowKey key = decode(data, MetricsRowKeySerializer.get());
        return Response.status(Response.Status.OK).entity(key).build();
    }

    @POST
    @Path("/encode-row-key")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public Response encodeRowKey(final MetricsRowKey key) {
        final String data = encode(key, MetricsRowKeySerializer.get());
        return Response.status(Response.Status.OK).entity(data).build();
    }

    @POST
    @Path("/decode-cache-key")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    public Response decodeCacheKey(final String data) {
        final CacheKey key = decode(data, CacheKeySerializer.get());
        return Response.status(Response.Status.OK).entity(key).build();
    }

    @POST
    @Path("/encode-cache-key")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public Response encodeCacheKey(final CacheKey key) {
        final String data = encode(key, CacheKeySerializer.get());
        return Response.status(Response.Status.OK).entity(data).build();
    }

    private <T> String encode(final T key, Serializer<T> serializer) {
        final ByteBuffer buffer = serializer.toByteBuffer(key);

        final byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        final String data = "0x" + BaseEncoding.base16().encode(bytes).toLowerCase();
        return data;
    }

    private <T> T decode(String data, Serializer<T> serializer) {
        if (data.substring(0, 2).equals("0x"))
            data = data.substring(2, data.length());

        data = data.toUpperCase();

        final byte[] bytes = BaseEncoding.base16().decode(data);
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return serializer.fromByteBuffer(buffer);
    }
}
