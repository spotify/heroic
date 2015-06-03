package com.spotify.heroic.metric.astyanax;

import lombok.RequiredArgsConstructor;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;

@RequiredArgsConstructor
public final class Context {
    protected final AstyanaxContext<Keyspace> context;
    protected final Keyspace client;
}