package com.spotify.heroic.aggregationcache.cassandra2;

import lombok.Data;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;

@Data
public final class Context {
    protected final AstyanaxContext<Keyspace> context;
    protected final Keyspace client;
}