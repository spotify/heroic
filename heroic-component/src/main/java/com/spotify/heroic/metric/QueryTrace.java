package com.spotify.heroic.metric;

import java.util.List;

import lombok.Data;

@Data
public class QueryTrace {
    private final List<String> details;
}