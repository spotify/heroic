package com.spotify.heroic.metadata.memory;

import com.spotify.heroic.metadata.MetadataModule;
import com.spotify.heroic.test.AbstractMetadataBackendIT;

public class MemoryBackendIT extends AbstractMetadataBackendIT {
    @Override
    protected MetadataModule setupModule() throws Exception {
        return MemoryMetadataModule.builder().build();
    }
}
