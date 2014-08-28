package com.spotify.heroic.http.write;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

import lombok.Data;

import com.spotify.heroic.http.HttpAsyncUtils;
import com.spotify.heroic.metrics.MetricBackendManager;
import com.spotify.heroic.metrics.MetricWriteException;
import com.spotify.heroic.metrics.model.WriteBatchResult;
import com.spotify.heroic.model.WriteMetric;

@Path("/write")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class WriteResource {
    @Inject
    private MetricBackendManager metrics;

    @Data
    public static final class Message {
        private final String message;
    }

    private static final HttpAsyncUtils.Resume<WriteBatchResult, WriteBatchResult> WRITE_METRICS = new HttpAsyncUtils.Resume<WriteBatchResult, WriteBatchResult>() {
        @Override
        public WriteBatchResult resume(WriteBatchResult result)
                throws Exception {
            return result;
        }
    };

    @POST
    @Path("/metrics")
    public void metrics(@Suspended final AsyncResponse response,
            WriteMetrics write) throws MetricWriteException {
        final WriteMetric entry = new WriteMetric(write.getSeries(),
                write.getData());

        final List<WriteMetric> writes = new ArrayList<WriteMetric>();
        writes.add(entry);

        HttpAsyncUtils.handleAsyncResume(response, metrics.write(writes),
                WRITE_METRICS);
    }
}
