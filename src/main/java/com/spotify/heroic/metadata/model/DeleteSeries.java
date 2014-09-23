package com.spotify.heroic.metadata.model;

import java.util.Collection;

import lombok.Data;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;

@Data
public class DeleteSeries {
    public static final DeleteSeries EMPTY = new DeleteSeries(0, 0);

    private final int successful;
    private final int failed;

    public static class Reducer implements
    Callback.Reducer<DeleteSeries, DeleteSeries> {
        @Override
        public DeleteSeries resolved(Collection<DeleteSeries> results,
                Collection<Exception> errors, Collection<CancelReason> cancelled)
                        throws Exception {

            if (!errors.isEmpty() || !cancelled.isEmpty())
                throw new Exception("Delete failed");

            int successful = 0;
            int failed = 0;

            for (final DeleteSeries result : results) {
                successful += result.getSuccessful();
                failed += result.getFailed();
            }

            return new DeleteSeries(successful, failed);
        }
    };

    private static final Reducer reducer = new Reducer();

    public static Reducer reduce() {
        return reducer;
    }
}