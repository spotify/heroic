package com.spotify.heroic.backend.list;

import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import com.spotify.heroic.backend.model.PreparedGroup;
import com.spotify.heroic.model.TimeSerie;

@RequiredArgsConstructor
public class RowGroups {
    @Getter
    private final Map<TimeSerie, List<PreparedGroup>> groups;
}
