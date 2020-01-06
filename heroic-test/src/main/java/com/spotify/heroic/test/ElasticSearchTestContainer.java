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

package com.spotify.heroic.test;

import java.net.InetSocketAddress;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

/*
 Create a singleton for metadata and suggest IT tests.
  One container should be be started instead of two.
*/
public class ElasticSearchTestContainer {
  private static ElasticSearchTestContainer instance = null;

  private ElasticsearchContainer esContainer;

  public InetSocketAddress getTcpHost() {
    return esContainer.getTcpHost();
  }

  private ElasticSearchTestContainer() {
    esContainer = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:7.5.0");
    esContainer.start();
  }

  public static synchronized ElasticSearchTestContainer getInstance() {
    if (instance == null) {
      instance = new ElasticSearchTestContainer();
    }
    return instance;
  }
}
