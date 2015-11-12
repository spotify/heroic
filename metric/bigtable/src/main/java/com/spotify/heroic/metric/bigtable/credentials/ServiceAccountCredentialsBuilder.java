/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.metric.bigtable.credentials;

import static com.google.common.base.Preconditions.checkNotNull;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.spotify.heroic.metric.bigtable.CredentialsBuilder;

@ToString(of = { "serviceAccount", "keyFile" })
public class ServiceAccountCredentialsBuilder implements CredentialsBuilder {
    private final String serviceAccount;
    private final String keyFile;

    @JsonCreator
    public ServiceAccountCredentialsBuilder(@JsonProperty("serviceAccount") String serviceAccount,
            @JsonProperty("keyFile") String keyFile) {
        this.serviceAccount = checkNotNull(serviceAccount, "serviceAccount");
        this.keyFile = checkNotNull(keyFile, "keyFile");
    }

    @Override
    public CredentialOptions build() {
        return CredentialOptions.p12Credential(serviceAccount, keyFile);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String serviceAccount;
        private String keyFile;

        public Builder serviceAccount(String serviceAccount) {
            this.serviceAccount = checkNotNull(serviceAccount, "serviceAccount");
            return this;
        }

        public Builder keyFile(String keyFile) {
            this.keyFile = checkNotNull(keyFile, "keyFile");
            return this;
        }

        public ServiceAccountCredentialsBuilder build() {
            return new ServiceAccountCredentialsBuilder(serviceAccount, keyFile);
        }
    }
}
