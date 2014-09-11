package com.spotify.heroic.metadata.model;

import java.util.HashSet;
import java.util.Set;

import lombok.Data;

@Data
public class FindKeys {
    public static final FindKeys EMPTY = new FindKeys(new HashSet<String>(), 0);

    private final Set<String> keys;
    private final int size;
}