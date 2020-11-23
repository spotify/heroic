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

package com.spotify.heroic.common;

import com.google.common.base.Strings;
import com.spotify.heroic.metric.QueryError;
import com.spotify.heroic.metric.RequestError;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple utility class to help with handling identified & anonymous Heroic API
 * requests.
 */
public class MandatoryClientIdUtil {

    private static final Logger log = LoggerFactory.getLogger(MandatoryClientIdUtil.class);

    /**
     * Driven by heroic.yaml's metrics.backends.anonymousRequestSeverity, this
     * enum embodies the three ways by which Heroic can treat an anonymous API
     * request, where an "anonymous request" is one without a Client ID.
     */
    public enum RequestInfractionSeverity {
        PERMIT,     // permit anonymous requests
        WARNING,    // perform query and also return a warning for anonymous requests
        REJECT      // anonymous requests are rejected with a warning message
    }

    /**
     * Converts the config - if present - into one of the three SEVERITY_*
     * values. It logs an error if invalid config is received and sets the severity
     * to SEVERITY_REJECT.
     *
     * @param severityFromConfig how severely a request missing a Client ID
     *                                  header is treated.
     * @return the according RequestInfractionSeverity
     */
    public static RequestInfractionSeverity parseAnonymousRequestSeverityConfig(
        String severityFromConfig) {

        if (Strings.isNullOrEmpty(severityFromConfig)) {
            return RequestInfractionSeverity.REJECT;
        }

        final var lowerCaseLevel = severityFromConfig.toLowerCase();

        if (lowerCaseLevel.contains("warn")) {
            return RequestInfractionSeverity.WARNING;
        } else if (lowerCaseLevel.contains("reject")) {
            return RequestInfractionSeverity.REJECT;
        } else if (lowerCaseLevel.contains("permit")) {
            return RequestInfractionSeverity.PERMIT;
        }

        var enums = Stream.of(RequestInfractionSeverity.values())
            .map(Enum::toString)
            .collect(Collectors.joining(", "));

        // OK, if we reach here, we were given something but we don't know what
        // it is, hence we log it out and take the safe option - reject.
        log.error("Received an unsupported configuration for severityFromConfig: '"
            + severityFromConfig + ". It must be one of: [" + enums + "].");

        return RequestInfractionSeverity.REJECT;
    }

    public static RequestError createMandatoryClientIdWarningError(
        RequestInfractionSeverity severity) {
        return new QueryError("The Client ID HTTP Header "
            + requestErrorToVerb(severity) +  " be supplied.");
    }

    private static String requestErrorToVerb(RequestInfractionSeverity severity) {
        switch (severity) {
            case REJECT:
                return "must";
            case WARNING:
                return "in future, must";
            case PERMIT:
                return "should";
            default:
                throw new RuntimeException(
                    "Severe logic failure: unexpected RequestInfractionSeverity "
                        + "enum value " + severity);
        }
    }
}
