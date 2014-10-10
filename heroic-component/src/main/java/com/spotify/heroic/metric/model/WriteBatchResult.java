package com.spotify.heroic.metric.model;

import java.util.Collection;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.Reducer;
import com.spotify.heroic.async.Transformer;

@Data
public class WriteBatchResult {
    private final boolean ok;
    private final int requests;

    public WriteBatchResult merge(WriteBatchResult other) {
        return new WriteBatchResult(this.ok && other.ok, this.requests + other.requests);
    }

    @Slf4j
    private static class Merger implements Reducer<WriteBatchResult, WriteBatchResult> {
        @Override
        public WriteBatchResult resolved(Collection<WriteBatchResult> results, Collection<Exception> errors,
                Collection<CancelReason> cancelled) throws Exception {
            for (final Exception e : errors)
                log.error("Write batch failed", e);

            for (final CancelReason cancel : cancelled)
                log.error("Write batch cancelled: {}", cancel);

            WriteBatchResult result = new WriteBatchResult(errors.isEmpty() && cancelled.isEmpty(), 0);

            for (final WriteBatchResult r : results) {
                result = result.merge(r);
            }

            return result;
        }
    }

    private static final Merger merger = new Merger();

    public static Merger merger() {
        return merger;
    }

    private static class ToBoolean implements Transformer<WriteBatchResult, Boolean> {
        @Override
        public Boolean transform(WriteBatchResult result) throws Exception {
            return result.isOk();
        }
    }

    private static final ToBoolean toBoolean = new ToBoolean();

    public static ToBoolean toBoolean() {
        return toBoolean;
    }
}
