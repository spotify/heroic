package com.spotify.heroic.elasticsearch;

import java.io.IOException;
import java.util.Map;

public interface BackendType<T> {
    Map<String, Map<String, Object>> mappings() throws IOException;

    Map<String, Object> settings() throws IOException;

    Class<? extends T> type();
}