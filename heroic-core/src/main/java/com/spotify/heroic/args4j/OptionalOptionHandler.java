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

package com.spotify.heroic.args4j;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.OptionDef;
import org.kohsuke.args4j.spi.OptionHandler;
import org.kohsuke.args4j.spi.Parameters;
import org.kohsuke.args4j.spi.Setter;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class OptionalOptionHandler extends OptionHandler<Optional<?>> {
    private static final Type STRING = String.class;
    private static final Type INTEGER = Integer.class;
    private static final Type LONG = Long.class;
    private static final Type PATH = Path.class;

    final Type type;

    public OptionalOptionHandler(
        CmdLineParser parser, OptionDef option, Setter<? super Optional<?>> setter
    ) {
        super(parser, option, setter);

        final Field f = (Field) setter.asFieldSetter().asAnnotatedElement();
        final ParameterizedType p = (ParameterizedType) f.getGenericType();
        this.type = p.getActualTypeArguments()[0];
    }

    @Override
    public String getDefaultMetaVariable() {
        return "[string]";
    }

    @Override
    public int parseArguments(Parameters params) throws CmdLineException {
        if (type.equals(STRING)) {
            setter.addValue(Optional.of(params.getParameter(0)));
            return 1;
        }

        if (type.equals(PATH)) {
            final Path p = Paths.get(params.getParameter(0));
            setter.addValue(Optional.of(p));
            return 1;
        }

        if (type.equals(INTEGER)) {
            setter.addValue(Optional.of(Integer.parseInt(params.getParameter(0))));
            return 1;
        }

        if (type.equals(LONG)) {
            setter.addValue(Optional.of(Long.parseLong(params.getParameter(0))));
            return 1;
        }

        throw new IllegalArgumentException("Unsupported type: " + type);
    }
}
