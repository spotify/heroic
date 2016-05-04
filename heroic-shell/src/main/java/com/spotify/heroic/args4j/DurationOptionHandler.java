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

import com.spotify.heroic.common.Duration;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.OptionDef;
import org.kohsuke.args4j.spi.OptionHandler;
import org.kohsuke.args4j.spi.Parameters;
import org.kohsuke.args4j.spi.Setter;

public class DurationOptionHandler extends OptionHandler<Duration> {
    public DurationOptionHandler(
        CmdLineParser parser, OptionDef option, Setter<? super Duration> setter
    ) {
        super(parser, option, setter);
    }

    @Override
    public int parseArguments(final Parameters params) throws CmdLineException {
        setter.addValue(Duration.parseDuration(params.getParameter(0)));
        return 1;
    }

    @Override
    public String getDefaultMetaVariable() {
        return "<number><w|d|h|m|s|ms>";
    }
}
