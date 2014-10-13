package com.spotify.heroic.httpclient.model;

import lombok.Data;

@Data
public class IdResponse<T> {
    private final T id;
}
