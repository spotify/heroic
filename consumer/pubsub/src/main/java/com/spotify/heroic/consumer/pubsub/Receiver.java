/*
 * Copyright (c) 2018 Spotify AB.
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

package com.spotify.heroic.consumer.pubsub;

import static io.opencensus.trace.AttributeValue.stringAttributeValue;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.spotify.heroic.consumer.ConsumerSchema;
import com.spotify.heroic.consumer.ConsumerSchemaValidationException;
import com.spotify.heroic.statistics.ConsumerReporter;
import com.spotify.heroic.statistics.FutureReporter;
import io.opencensus.common.Scope;
import io.opencensus.trace.Span;
import io.opencensus.trace.Status;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;

class Receiver implements MessageReceiver {

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(Receiver.class);
    private final ConsumerSchema.Consumer consumer;
    private final ConsumerReporter reporter;
    private final AtomicLong errors;
    private final LongAdder consumed;
    private final Tracer tracer = Tracing.getTracer();

    Receiver(
        final ConsumerSchema.Consumer consumer,
        final ConsumerReporter reporter,
        final AtomicLong errors,
        final LongAdder consumed
    ) {
        this.consumer = consumer;
        this.reporter = reporter;
        this.errors = errors;
        this.consumed = consumed;
    }

    @Override
    public void receiveMessage(PubsubMessage message, AckReplyConsumer replyConsumer) {
        // handle incoming message, then ack/nack the received message
        final ByteString data = message.getData();
        final String messageId = message.getMessageId();
        log.debug("Received ID:{} with content: {}", messageId, data.toStringUtf8());
        final byte[] bytes = data.toByteArray();

        Span span = tracer.spanBuilder("PubSub.receiveMessage").startSpan();
        span.putAttribute("id", stringAttributeValue(messageId));

        final FutureReporter.Context consumptionContext = reporter.reportConsumption();

        // process the data
        try (Scope ws = tracer.withSpan(span)) {
            consumer.consume(bytes).onDone(consumptionContext).onFinished(() -> {
                reporter.reportMessageSize(bytes.length);
                replyConsumer.ack();
                span.end();
            });
        } catch (ConsumerSchemaValidationException e) {
            reporter.reportConsumerSchemaError();
            log.error("ID:{} - {}", messageId, e.getMessage(), e);

            // The message will never be processable, ack it to make it go away
            replyConsumer.ack();
            span.end();
        } catch (Exception e) {
            errors.incrementAndGet();
            log.error("ID:{} - Failed to consume", messageId, e);
            span.setStatus(Status.INTERNAL.withDescription(e.toString()));
            reporter.reportMessageError();
            replyConsumer.nack();
            span.end();
        } finally {
            consumed.increment();
        }
  }

}
