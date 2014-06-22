package com.spotify.heroic.http.model;

import lombok.Data;

import com.spotify.heroic.model.TimeSerie;

@Data
public class DecodeRowKeyResponse {
    private final TimeSerie timeSerie;
    private final long base;
}
