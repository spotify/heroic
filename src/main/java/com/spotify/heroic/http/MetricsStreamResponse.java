package com.spotify.heroic.http;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class MetricsStreamResponse {
    @Getter
    private final String id;
}
