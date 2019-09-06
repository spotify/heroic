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

package com.spotify.heroic.consumer.collectd

import com.spotify.heroic.consumer.collectd.CollectdTypes.Field

interface CollectdValue {
    fun convert(field: Field): Double
    fun toDouble(): Double
}

data class Gauge(val value: Double): CollectdValue {
    override fun convert(field: Field): Double = field.convertGauge(value)
    override fun toDouble(): Double = value
}

data class Absolute(val value: Long): CollectdValue {
    override fun convert(field: Field): Double = field.convertAbsolute(value)
    override fun toDouble(): Double = value.toDouble()
}

data class Derive(val derivate: Long): CollectdValue {
    override fun convert(field: Field): Double = field.convertAbsolute(derivate)
    override fun toDouble(): Double = derivate.toDouble()
}

data class Counter(val counter: Long): CollectdValue {
    override fun convert(field: Field): Double = field.convertAbsolute(counter)
    override fun toDouble(): Double = counter.toDouble()
}