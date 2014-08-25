package com.spotify.heroic.http.rpc;

import java.util.List;

import lombok.Data;

import com.spotify.heroic.model.DataPoint;

@Data
public class RpcQuery {
    private final List<DataPoint> dataPoints;
}
