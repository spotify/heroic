package com.spotify.heroic.grammar;

import java.util.List;

import lombok.Data;

@Data
public final class Queries {
    private final List<QueryDSL> queries;
}