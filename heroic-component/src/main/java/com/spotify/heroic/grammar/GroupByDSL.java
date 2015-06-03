package com.spotify.heroic.grammar;

import java.util.List;

import lombok.Data;

@Data
public class GroupByDSL {
    private final List<String> groupBy;
}
