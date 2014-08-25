package com.spotify.heroic.http.utils;

import lombok.Data;

import com.spotify.heroic.model.Series;

@Data
public class UtilsRowKeyResponse {
    private final Series series;
    private final long base;
}
