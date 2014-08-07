package com.spotify.heroic.http.rpc.model;

import java.util.List;

import lombok.Data;

import com.spotify.heroic.model.DataPoint;

@Data
public class RpcQueryResponse {
    private final List<DataPoint> dataPoints;
}
