package com.spotify.heroic.metric.bigtable.credentials;

import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.file.Files;
import java.nio.file.Path;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.spotify.heroic.metric.bigtable.CredentialsBuilder;

import lombok.ToString;

@ToString(of = { "path" })
public class JsonCredentialsBuilder implements CredentialsBuilder {
    private final Path path;

    @JsonCreator
    public JsonCredentialsBuilder(@JsonProperty("path") Path path) {
        this.path = checkNotNull(path, "path");
    }

    @Override
    public CredentialOptions build() throws Exception {
        // XXX: You have to leave the input stream open for BigtableSession to use it.
        //      This does 'leak' an input stream, but it's only once, so we'll live with it for now.
        //      Reported here: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/issues/534
        return CredentialOptions.jsonCredentials(Files.newInputStream(path));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Path path;

        public Builder path(Path path) {
            checkNotNull(path, "path");

            if (!Files.isReadable(path)) {
                throw new IllegalArgumentException("Path must be readable: " + path.toAbsolutePath());
            }

            this.path = path;
            return this;
        }

        public JsonCredentialsBuilder build() {
            return new JsonCredentialsBuilder(path);
        }
    }
}