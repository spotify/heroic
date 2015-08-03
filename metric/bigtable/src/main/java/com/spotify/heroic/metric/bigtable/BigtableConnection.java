package com.spotify.heroic.metric.bigtable;

import com.spotify.heroic.metric.bigtable.api.BigtableClient;
import com.spotify.heroic.metric.bigtable.api.BigtableTableAdminClient;

public interface BigtableConnection {
    public BigtableTableAdminClient adminClient();

    public BigtableClient client();

    public void close() throws Exception;
}