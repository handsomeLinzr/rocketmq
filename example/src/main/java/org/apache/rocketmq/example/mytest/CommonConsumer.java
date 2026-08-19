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
import java.util.List;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * Demonstrates concurrent and orderly push consumers.
 */
public final class CommonConsumer {
    private CommonConsumer() {
    }

    /**
     * Starts a concurrent consumer; keep the process alive to receive messages.
     *
     * @param args ignored command-line arguments
     * @throws Exception when consumer startup fails
     */
    public static void main(String[] args) throws Exception {
        startConcurrentConsumer();
    }

    /**
     * Starts a concurrent consumer for all tags on the common topic.
     *
     * @throws Exception when consumer startup fails
     */
    public static void startConcurrentConsumer() throws Exception {
        DefaultMQPushConsumer consumer = createConsumer("mytest-concurrent-consumer");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages,
                ConsumeConcurrentlyContext context) {
                for (MessageExt message : messages) {
                    printMessage(message);
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.println("concurrent consumer started");
        waitForever();
    }

    /**
     * Starts an orderly consumer where messages from the same queue are serialized.
     *
     * @throws Exception when consumer startup fails
     */
    public static void startOrderlyConsumer() throws Exception {
        DefaultMQPushConsumer consumer = createConsumer("mytest-orderly-consumer");
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> messages, ConsumeOrderlyContext context) {
                for (MessageExt message : messages) {
                    printMessage(message);
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        consumer.start();
        System.out.println("orderly consumer started");
        waitForever();
    }

    /**
     * Creates a consumer subscribed from the earliest available offset.
     *
     * @param consumerGroup unique consumer group name
     * @return configured consumer
     * @throws Exception when subscription configuration fails
     */
    private static DefaultMQPushConsumer createConsumer(String consumerGroup) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(RocketMqConfig.NAMESRV_ADDR);
        consumer.subscribe(RocketMqConfig.COMMON_TOPIC, "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        return consumer;
    }

    /**
     * Prints the fields that are usually useful while debugging consumption.
     *
     * @param message received message
     */
    private static void printMessage(MessageExt message) {
        String body = new String(message.getBody(), StandardCharsets.UTF_8);
        System.out.printf("topic=%s, tags=%s, keys=%s, msgId=%s, body=%s%n",
            message.getTopic(), message.getTags(), message.getKeys(), message.getMsgId(), body);
    }

    /**
     * Keeps a demonstration consumer process alive.
     *
     * @throws InterruptedException when the process is interrupted
     */
    private static void waitForever() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }
}
