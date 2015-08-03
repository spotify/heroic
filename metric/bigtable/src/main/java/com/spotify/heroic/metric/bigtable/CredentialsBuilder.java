package com.spotify.heroic.metric.bigtable;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.spotify.heroic.metric.bigtable.credentials.ComputeEngineCredentialsBuilder;
import com.spotify.heroic.metric.bigtable.credentials.ServiceAccountCredentialsBuilder;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = ComputeEngineCredentialsBuilder.class, name = "compute-engine"),
        @JsonSubTypes.Type(value = ServiceAccountCredentialsBuilder.class, name = "service-account") })
public interface CredentialsBuilder {
    public CredentialOptions build();
}