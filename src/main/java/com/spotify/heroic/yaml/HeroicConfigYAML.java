package com.spotify.heroic.yaml;

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;

import com.spotify.heroic.backend.Backend;
import com.spotify.heroic.backend.BackendManager;
import com.spotify.heroic.backend.ListBackendManager;

public class HeroicConfigYAML {
    @Getter
    @Setter
    private List<Backend.YAML> backends;

    @Getter
    @Setter
    private long backendTimeout = HeroicConfig.DEFAULT_TIMEOUT;

    private List<Backend> setupBackends(String context)
            throws ValidationException {
        List<Backend> backends = new ArrayList<Backend>();

        int i = 0;

        for (Backend.YAML backend : Utils.toList("backends", this.backends)) {
            backends.add(backend.build("backends[" + i++ + "]"));
        }

        return backends;
    }

    public HeroicConfig build() throws ValidationException {
        final List<Backend> backends = setupBackends("backends");
        final BackendManager backendManager = new ListBackendManager(backends,
                backendTimeout);
        return new HeroicConfig(backendManager);
    }
}
