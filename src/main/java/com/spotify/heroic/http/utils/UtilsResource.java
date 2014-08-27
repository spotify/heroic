package com.spotify.heroic.http.utils;

import java.nio.ByteBuffer;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.io.BaseEncoding;
import com.spotify.heroic.http.general.DataResponse;
import com.spotify.heroic.metrics.heroic.MetricsRowKey;
import com.spotify.heroic.metrics.heroic.MetricsRowKeySerializer;

@Path("/utils")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class UtilsResource {
    /**
     * Encode/Decode functions, helpful when interacting with cassandra through
     * cqlsh.
     */
    @POST
    @Path("/decode-row-key")
    @Produces(MediaType.APPLICATION_JSON)
    public Response decodeRowKey(final UtilsDecodeRowKeyQuery request) {
        String data = request.getData();

        if (data.substring(0, 2).equals("0x"))
            data = data.substring(2, data.length());

        data = data.toUpperCase();

        final byte[] bytes = BaseEncoding.base16().decode(data);
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        final MetricsRowKey rowKey = MetricsRowKeySerializer.get()
                .fromByteBuffer(buffer);
        return Response.status(Response.Status.OK)
                .entity(new UtilsRowKeyResponse(rowKey.getSeries(), rowKey.getBase()))
                .build();
    }

    @POST
    @Path("/encode-row-key")
    @Produces(MediaType.APPLICATION_JSON)
    public Response encodeRowKey(final UtilsEncodeRowKeyQuery request) {
        final MetricsRowKey rowKey = new MetricsRowKey(request.getSeries(),
                request.getBase());
        final ByteBuffer buffer = MetricsRowKeySerializer.get().toByteBuffer(
                rowKey);

        final byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        final String data = "0x"
                + BaseEncoding.base16().encode(bytes).toLowerCase();
        return Response.status(Response.Status.OK)
                .entity(new DataResponse<String>(data)).build();
    }
}
