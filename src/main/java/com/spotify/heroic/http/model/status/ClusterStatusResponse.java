package com.spotify.heroic.http.model.status;

import lombok.Data;

@Data
public class ClusterStatusResponse {
	private final boolean ok;
	private final int onlineNodes;
	private final int offlineNodes;
}
