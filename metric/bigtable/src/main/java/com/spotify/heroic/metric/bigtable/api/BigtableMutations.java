package com.spotify.heroic.metric.bigtable.api;

import java.util.List;

import lombok.Data;

import com.google.bigtable.v1.Mutation;

@Data
public class BigtableMutations {
    final List<Mutation> mutations;
}