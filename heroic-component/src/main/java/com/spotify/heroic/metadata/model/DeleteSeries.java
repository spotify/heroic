package com.spotify.heroic.metadata.model;

import java.util.Collection;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import eu.toolchain.async.Collector;

@Data
public class DeleteSeries {
    public static final DeleteSeries EMPTY = new DeleteSeries(0, 0);

    private final int deleted;
    private final int failed;

    public static class SelfReducer implements Collector<DeleteSeries, DeleteSeries> {
        @Override
        public DeleteSeries collect(Collection<DeleteSeries> results) throws Exception {
            int deleted = 0;
            int failed = 0;

            for (final DeleteSeries result : results) {
                deleted += result.getDeleted();
                failed += result.getFailed();
            }

            return new DeleteSeries(deleted, failed);
        }
    };

    private static final SelfReducer reducer = new SelfReducer();

    public static Collector<DeleteSeries, DeleteSeries> reduce() {
        return reducer;
    }

    @JsonCreator
    public static DeleteSeries create(@JsonProperty("deleted") int deleted, @JsonProperty("failed") int failed) {
        return new DeleteSeries(deleted, failed);
    }
}