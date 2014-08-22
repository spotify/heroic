package com.spotify.heroic.http.model.status;

import lombok.Data;

@Data
public class ConsumerStatusResponse {
	private final boolean ok;
	private final int available;
	private final int ready;
}
