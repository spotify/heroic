package com.spotify.heroic.http.general;

import lombok.Data;

@Data
public class DataResponse<T> {
    private final T data;
}
