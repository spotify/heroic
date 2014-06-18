package com.spotify.heroic.metrics.model;

import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import com.spotify.heroic.model.TimeSerie;

@RequiredArgsConstructor
public class RowGroups {
    @Getter
    private final Map<TimeSerie, List<PreparedGroup>> groups;
}
