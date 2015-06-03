package com.spotify.heroic.grammar;

public interface Context {
    public RuntimeException error(String message);

    public RuntimeException error(Exception cause);
}