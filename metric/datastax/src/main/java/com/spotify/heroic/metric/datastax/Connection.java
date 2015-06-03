package com.spotify.heroic.metric.datastax;

import lombok.RequiredArgsConstructor;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

@RequiredArgsConstructor
public final class Connection {
    protected final Cluster cluster;
    protected final Session session;
    protected final PreparedStatement write;
    protected final PreparedStatement fetch;
    protected final PreparedStatement keysUnbound;
    protected final PreparedStatement keysLeftbound;
    protected final PreparedStatement keysRightbound;
    protected final PreparedStatement keysBound;
}