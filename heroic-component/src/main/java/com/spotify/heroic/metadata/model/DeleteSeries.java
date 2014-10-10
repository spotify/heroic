package com.spotify.heroic.metadata.model;

import java.util.Collection;

import lombok.Data;

import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.Reducer;

@Data
public class DeleteSeries {
    public static final DeleteSeries EMPTY = new DeleteSeries(0);

    private final int deleted;

    public static class SelfReducer implements Reducer<DeleteSeries, DeleteSeries> {
        @Override
        public DeleteSeries resolved(Collection<DeleteSeries> results, Collection<Exception> errors,
                Collection<CancelReason> cancelled) throws Exception {

            if (!errors.isEmpty() || !cancelled.isEmpty())
                throw new Exception("Delete failed");

            int deleted = 0;

            for (final DeleteSeries result : results) {
                deleted += result.getDeleted();
            }

            return new DeleteSeries(deleted);
        }
    };

    private static final SelfReducer reducer = new SelfReducer();

    public static Reducer<DeleteSeries, DeleteSeries> reduce() {
        return reducer;
    }
}