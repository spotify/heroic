package com.spotify.heroic.httpclient.model;

import lombok.Data;

@Data
public class DataResponse<T> {
    private final T data;
}
