/*
 * Copyright (c) 2020 Spotify AB.
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

package com.spotify.heroic.tracing;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Class copied from version 0.107.0 of
 * https://github.com/googleapis/google-cloud-java/blob/master/
 * google-cloud-clients/google-cloud-core/src/main/java/com/google/cloud/MetadataConfig.java
 * to avoid dependencies issues with such a large library. The only thing we care about from that
 * library is this class to read the metadata service.
 */
public class GcpMetadataLookup {
  private static final String METADATA_URL = "http://metadata.google.internal/computeMetadata/v1/";
  private static final int TIMEOUT_MS = 5000;

  private GcpMetadataLookup() { }

  public static String getProjectId() {
    return getAttribute("project/project-id");
  }

  public static String getZone() {
    String zoneId = getAttribute("instance/zone");
    if (zoneId != null && zoneId.contains("/")) {
      return zoneId.substring(zoneId.lastIndexOf('/') + 1);
    }
    return zoneId;
  }

  public static String getAttribute(String attributeName) {
    try {
      URL url = new URL(METADATA_URL + attributeName);
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setConnectTimeout(TIMEOUT_MS);
      connection.setReadTimeout(TIMEOUT_MS);
      connection.setRequestProperty("Metadata-Flavor", "Google");
      try (InputStream input = connection.getInputStream()) {
        if (connection.getResponseCode() == 200) {
          try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, UTF_8))) {
            return reader.readLine();
          }
        }
      }
    } catch (IOException ignore) {
      // ignore
    }
    return null;
  }
}
