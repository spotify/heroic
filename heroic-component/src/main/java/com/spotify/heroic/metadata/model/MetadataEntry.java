package com.spotify.heroic.metadata.model;

import lombok.Data;

import com.spotify.heroic.model.Series;

@Data
public class MetadataEntry {
    private final Series series;
}