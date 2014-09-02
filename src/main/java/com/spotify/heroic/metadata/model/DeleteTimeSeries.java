package com.spotify.heroic.metadata.model;

import lombok.Data;

@Data
public class DeleteTimeSeries {
    private final int successful;
    private final int failed;
}