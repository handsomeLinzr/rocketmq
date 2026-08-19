/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.mytest;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * Demonstrates synchronous, asynchronous, one-way, delayed, and batch sends.
 *
 * <p>Run one method at a time from an IDE, or run {@link #main(String[])} to
 * execute all methods. The Broker must allow writes before running it.</p>
 */
public final class CommonProducer {
    private CommonProducer() {
    }

    /**
     * Runs the common producer demonstrations.
     *
     * @param args ignored command-line arguments
     * @throws Exception when producer startup or sending fails
     */
    public static void main(String[] args) throws Exception {
        sendSynchronously();
        sendAsynchronously();
        sendOneWay();
        sendDelayed();
        sendBatch();
    }

    /**
     * Sends a message and waits for the Broker response.
     *
     * @throws Exception when sending fails
     */
    public static void sendSynchronously() throws Exception {
        DefaultMQProducer producer = createProducer("mytest-sync-producer");
        try {
            Message message = message("sync-tag", "sync-key", "同步消息");
            SendResult result = producer.send(message);
            System.out.println("sync: " + result);
        } finally {
            producer.shutdown();
        }
    }

    /**
     * Sends a message through a callback without blocking the caller.
     *
     * @throws Exception when producer startup or callback waiting fails
     */
    public static void sendAsynchronously() throws Exception {
        DefaultMQProducer producer = createProducer("mytest-async-producer");
        CountDownLatch latch = new CountDownLatch(1);
        try {
            producer.send(message("async-tag", "async-key", "异步消息"), new SendCallback() {
                @Override
                public void onSuccess(SendResult result) {
                    System.out.println("async: " + result);
                    latch.countDown();
                }

                @Override
                public void onException(Throwable exception) {
                    exception.printStackTrace();
                    latch.countDown();
                }
            });
            latch.await(10, TimeUnit.SECONDS);
        } finally {
            producer.shutdown();
        }
    }

    /**
     * Sends without waiting for a send result.
     *
     * @throws Exception when producer startup or sending fails
     */
    public static void sendOneWay() throws Exception {
        DefaultMQProducer producer = createProducer("mytest-oneway-producer");
        try {
            producer.sendOneway(message("oneway-tag", "oneway-key", "单向消息"));
            System.out.println("oneway: request sent");
        } finally {
            producer.shutdown();
        }
    }

    /**
     * Sends a message that becomes visible after five seconds.
     *
     * @throws Exception when producer startup or sending fails
     */
    public static void sendDelayed() throws Exception {
        DefaultMQProducer producer = createProducer("mytest-delay-producer");
        try {
            Message message = message("delay-tag", "delay-key", "延迟消息");
            message.setDelayTimeLevel(3);
            System.out.println("delay: " + producer.send(message));
        } finally {
            producer.shutdown();
        }
    }

    /**
     * Sends several messages in one batch request.
     *
     * @throws Exception when producer startup or sending fails
     */
    public static void sendBatch() throws Exception {
        DefaultMQProducer producer = createProducer("mytest-batch-producer");
        try {
            List<Message> messages = Arrays.asList(
                message("batch-tag", "batch-key-1", "批量消息-1"),
                message("batch-tag", "batch-key-2", "批量消息-2"));
            System.out.println("batch: " + producer.send(messages));
        } finally {
            producer.shutdown();
        }
    }

    /**
     * Creates and starts a producer with the shared local settings.
     *
     * @param producerGroup unique producer group name
     * @return started producer
     */
    private static DefaultMQProducer createProducer(String producerGroup) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr(RocketMqConfig.NAMESRV_ADDR);
        producer.start();
        return producer;
    }

    /**
     * Creates a UTF-8 message for the common topic.
     *
     * @param tag message tag
     * @param key business key
     * @param body message body
     * @return RocketMQ message
     */
    private static Message message(String tag, String key, String body) {
        return new Message(RocketMqConfig.COMMON_TOPIC, tag, key, body.getBytes(StandardCharsets.UTF_8));
    }
}
