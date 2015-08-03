package com.spotify.heroic.metric.bigtable.credentials;

import lombok.ToString;

import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.spotify.heroic.metric.bigtable.CredentialsBuilder;

@ToString
public class ComputeEngineCredentialsBuilder implements CredentialsBuilder {
    @Override
    public CredentialOptions build() {
        return CredentialOptions.credential(new ComputeEngineCredentials());
    }
}