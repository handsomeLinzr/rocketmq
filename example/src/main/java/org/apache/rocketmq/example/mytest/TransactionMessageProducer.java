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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * Demonstrates RocketMQ half messages, local transactions, and transaction checks.
 *
 * <p>The local transaction in this example is an in-memory simulation. In a real
 * application, persist the transaction state in a database and make the local
 * business operation idempotent.</p>
 */
public final class TransactionMessageProducer {
    private static final String PRODUCER_GROUP = "mytest-transaction-producer";
    private static final Map<String, LocalTransactionState> TRANSACTION_STATES = new ConcurrentHashMap<>();
    private static final AtomicInteger TRANSACTION_INDEX = new AtomicInteger();

    private TransactionMessageProducer() {
    }

    /**
     * Sends three transaction messages and waits for broker checks.
     *
     * @param args ignored command-line arguments
     * @throws Exception when producer startup or sending fails
     */
    public static void main(String[] args) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        TransactionMQProducer producer = new TransactionMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(RocketMqConfig.NAMESRV_ADDR);
        producer.setExecutorService(executor);
        producer.setTransactionListener(new DemoTransactionListener());
        producer.start();
        try {
            for (int i = 0; i < 3; i++) {
                Message message = new Message(RocketMqConfig.TRANSACTION_TOPIC,
                    "transaction-tag", "transaction-key-" + i,
                    ("transaction-body-" + i).getBytes(StandardCharsets.UTF_8));
                SendResult result = producer.sendMessageInTransaction(message, "order-" + i);
                System.out.println("transaction: " + result);
            }
            Thread.sleep(30000);
        } finally {
            producer.shutdown();
            executor.shutdown();
        }
    }

    /**
     * Executes and checks the local transaction state.
     */
    private static final class DemoTransactionListener implements TransactionListener {
        /**
         * Executes the local business operation after the half message is stored.
         *
         * @param message half message sent to the Broker
         * @param argument application business argument
         * @return commit, rollback, or unknown state
         */
        @Override
        public LocalTransactionState executeLocalTransaction(Message message, Object argument) {
            int index = TRANSACTION_INDEX.getAndIncrement();
            LocalTransactionState state = index == 1
                ? LocalTransactionState.ROLLBACK_MESSAGE
                : LocalTransactionState.COMMIT_MESSAGE;
            TRANSACTION_STATES.put(message.getTransactionId(), state);
            System.out.println("local transaction " + message.getTransactionId() + ": " + state);
            return state;
        }

        /**
         * Returns the persisted local transaction result when the Broker asks again.
         *
         * @param message transaction check message
         * @return known local transaction state, or unknown when it is not available
         */
        @Override
        public LocalTransactionState checkLocalTransaction(MessageExt message) {
            LocalTransactionState state = TRANSACTION_STATES.get(message.getTransactionId());
            return state == null ? LocalTransactionState.UNKNOW : state;
        }
    }
}
