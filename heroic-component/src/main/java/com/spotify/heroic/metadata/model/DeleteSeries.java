package com.spotify.heroic.metadata.model;

import java.util.Collection;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.Reducer;

@Data
public class DeleteSeries {
    public static final DeleteSeries EMPTY = new DeleteSeries(0);

    private final int deleted;

    public static class SelfReducer implements Reducer<DeleteSeries, DeleteSeries> {
        @Override
        public DeleteSeries resolved(Collection<DeleteSeries> results, Collection<CancelReason> cancelled)
                throws Exception {
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

    @JsonCreator
    public static DeleteSeries create(@JsonProperty("deleted") int deleted) {
        return new DeleteSeries(deleted);
    }
}