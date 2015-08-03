package com.spotify.heroic.metric.bigtable.api;

import lombok.Data;

import com.google.bigtable.v1.RowFilter;

@Data
public class BigtableRowFilter {
    
    final RowFilter filter; 
}
