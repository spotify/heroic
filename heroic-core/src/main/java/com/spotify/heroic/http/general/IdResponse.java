package com.spotify.heroic.http.general;

import lombok.Data;

@Data
public class IdResponse<T> {
    private final T id;
}
