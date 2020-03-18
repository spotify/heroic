package com.spotify.heroic.suggest.memory;

import com.spotify.heroic.suggest.SuggestModule;
import com.spotify.heroic.test.AbstractSuggestBackendIT;

public class MemoryBackendIT extends AbstractSuggestBackendIT {
    @Override
    protected SuggestModule setupModule() {
        return MemorySuggestModule.builder().build();
    }

    @Override
    public void writeDuplicatesReturnErrorInResponse() { }
}
