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

package com.spotify.heroic.shell;

import java.util.ArrayList;
import java.util.List;

import org.kohsuke.args4j.Option;

public abstract class AbstractShellTaskParams implements TaskParameters {
    @Option(name = "-c", aliases = {"--config"},
            usage = "Path to configuration (only used in standalone)", metaVar = "<config>")
    public String config;

    @Option(name = "-h", aliases = {"--help"}, help = true, usage = "Display help")
    public boolean help;

    @Option(name = "-o", aliases = {"--output"}, usage = "Redirect output to the given file",
            metaVar = "<file|->")
    public String output;

    @Option(name = "--gzip", usage = "Write output in gzip")
    public boolean gzip = false;

    @Option(name = "-P", aliases = {"--profile"}, usage = "Activate the given heroic profile",
            metaVar = "<profile>")
    public List<String> profiles = new ArrayList<>();

    @Override
    public String config() {
        return config;
    }

    @Override
    public boolean help() {
        return help;
    }

    @Override
    public boolean gzip() {
        return gzip;
    }

    @Override
    public String output() {
        return output;
    }

    @Override
    public List<String> profiles() {
        return profiles;
    }
}
