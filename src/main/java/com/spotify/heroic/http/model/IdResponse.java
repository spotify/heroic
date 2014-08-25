package com.spotify.heroic.http.model;

import lombok.Data;

@Data
public class IdResponse<T> {
    private final T id;
}
