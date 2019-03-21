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

package com.spotify.heroic.elasticsearch;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.async.Transform;
import javax.inject.Provider;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.index.engine.VersionConflictEngineException;

public class AbstractElasticsearchBackend {
    protected final AsyncFramework async;

    @java.beans.ConstructorProperties({ "async" })
    public AbstractElasticsearchBackend(final AsyncFramework async) {
        this.async = async;
    }

    protected <T extends ActionResponse> AsyncFuture<T> bind(
        final ListenableActionFuture<T> actionFuture
    ) {
        final ResolvableFuture<T> future = async.future();

        actionFuture.addListener(new ActionListener<T>() {
            @Override
            public void onResponse(T result) {
                future.resolve(result);
            }

            @Override
            public void onFailure(Exception e) {
                future.fail(e);
            }
        });

        return future;
    }

    protected <T> Transform<Throwable, T> handleVersionConflict(
        Provider<T> emptyProvider, Runnable reportWriteDroppedByDuplicate
    ) {
        return throwable -> {
            if (throwable instanceof VersionConflictEngineException) {
                // Index request rejected, document already exists. That's ok, return success.
                reportWriteDroppedByDuplicate.run();
                return emptyProvider.get();
            }
            throw new RuntimeException(throwable);
        };
    }
}
