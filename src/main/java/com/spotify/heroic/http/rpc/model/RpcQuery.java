package com.spotify.heroic.http.rpc.model;

import java.util.List;

import lombok.Data;

import com.spotify.heroic.model.DataPoint;

@Data
public class RpcQuery {
    private final List<DataPoint> dataPoints;
}
