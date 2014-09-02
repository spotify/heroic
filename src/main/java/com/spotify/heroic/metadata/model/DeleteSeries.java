package com.spotify.heroic.metadata.model;

import lombok.Data;

@Data
public class DeleteSeries {
    private final int successful;
    private final int failed;
}