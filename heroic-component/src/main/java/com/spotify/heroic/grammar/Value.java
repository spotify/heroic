package com.spotify.heroic.grammar;

public interface Value {
    public Value sub(Value other);

    public Value add(Value other);

    public <T> T cast(T to);

    public <T> T cast(Class<T> to);
}