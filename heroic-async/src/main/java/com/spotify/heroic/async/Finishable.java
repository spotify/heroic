package com.spotify.heroic.async;

public interface Finishable {
    void finished() throws Exception;
}