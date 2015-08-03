package com.spotify.heroic.metric.bigtable.api;

import java.io.IOException;
import java.util.List;

public interface BigtableTableAdminClient {
    /**
     * Create the specified table.
     */
    public void createTable(BigtableTable table) throws IOException;

    /**
     * Create the specified column family.
     */
    public void createColumnFamily(String table, BigtableColumnFamily family) throws IOException;

    /**
     * Get details about a table.
     */
    public BigtableTable getTable(String name) throws IOException;

    /**
     * High-level API that iterates through all tables, and fetches details.
     */
    public List<BigtableTable> listTablesDetails() throws IOException;

    /**
     * Create a builder for column families.
     */
    public BigtableColumnFamilyBuilder columnFamily(String name);

    /**
     * Create a builder for tables.
     */
    public BigtableTableBuilder table(String name);
}