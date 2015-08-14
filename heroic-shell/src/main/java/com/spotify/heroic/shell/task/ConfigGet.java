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

package com.spotify.heroic.shell.task;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;

import lombok.ToString;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.HeroicConfig;
import com.spotify.heroic.HeroicShell;
import com.spotify.heroic.shell.AbstractShellTask;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellTaskParams;
import com.spotify.heroic.shell.ShellTaskUsage;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

@ShellTaskUsage("Load metadata from a file")
public class ConfigGet extends AbstractShellTask {
    public static void main(String argv[]) throws Exception {
        HeroicShell.standalone(argv, ConfigGet.class);
    }

    @Inject
    private HeroicConfig config;

    @Inject
    @Named("application/json")
    private ObjectMapper mapper;

    @Inject
    private AsyncFramework async;

    @Override
    public ShellTaskParams params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final PrintWriter out, ShellTaskParams base) throws Exception {
        final Parameters params = (Parameters) base;
        final ObjectMapper m = mapper.copy();
        m.enable(SerializationFeature.INDENT_OUTPUT);
        out.println(m.writeValueAsString(config));

        return async.resolved();
    }

    private InputStreamReader open(Path file) throws IOException {
        final InputStream input = Files.newInputStream(file);

        // unpack gzip.
        if (!file.getFileName().toString().endsWith(".gz"))
            return new InputStreamReader(input);

        return new InputStreamReader(new GZIPInputStream(input));
    }

    @ToString
    private static class Parameters extends AbstractShellTaskParams {
    }
}