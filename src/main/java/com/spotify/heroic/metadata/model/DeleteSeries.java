package com.spotify.heroic.metadata.model;

import lombok.Data;

@Data
public class DeleteSeries {
    public static final DeleteSeries EMPTY = new DeleteSeries(0, 0);

    private final int successful;
    private final int failed;
}