package com.spotify.heroic.http.model;

import lombok.Data;

@Data
public class DataResponse<T> {
    private final T data;
}
