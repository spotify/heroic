package com.spotify.heroic.backend;

import com.codahale.metrics.MetricRegistry;
import com.spotify.heroic.yaml.ValidationException;

public interface Backend {
    public static interface YAML {
        Backend build(String context, MetricRegistry registry)
                throws ValidationException;
    }
}
