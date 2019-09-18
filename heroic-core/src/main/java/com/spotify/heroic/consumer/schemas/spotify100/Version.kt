/*
 * Copyright (c) 2019 Spotify AB.
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

package com.spotify.heroic.consumer.schemas.spotify100

import java.lang.Integer.parseInt

data class Version(val major: Int, val minor: Int, val patch: Int) {
    companion object {
        @JvmStatic fun parse(input: String): Version {
            val parts = input.trim().split(".")
            if (parts.size != 3) {
                throw IllegalArgumentException("too few components: $input")
            }
            return Version(parseInt(parts[0]), parseInt(parts[1]), parseInt(parts[2]))
        }
    }
}