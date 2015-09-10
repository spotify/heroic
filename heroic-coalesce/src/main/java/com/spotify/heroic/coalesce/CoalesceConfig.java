package com.spotify.heroic.coalesce;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import lombok.Data;

@Data
public class CoalesceConfig {
    private final List<CoalesceTask> tasks;

    @JsonCreator
    public CoalesceConfig(final List<CoalesceTask> tasks) {
        this.tasks = Optional.fromNullable(tasks).or(ImmutableList::of);
    }
}