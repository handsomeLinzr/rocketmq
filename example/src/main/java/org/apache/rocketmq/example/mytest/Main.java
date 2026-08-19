package org.apache.rocketmq.example.mytest;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * @author linzherong
 * @date 2026/8/19 13:19
 */
public class Main {

    @Test
    public void testSend() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer();
        producer.setNamesrvAddr("localhost:9876");
        producer.setProducerGroup("My-group");
        producer.start();

        SendResult sendResult = producer.send(new Message("ooxxA", "tag-A", "key-A","hello12345".getBytes()));
        System.out.println(sendResult);
    }

    @Test
    public void testConsume() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer();
        consumer.setNamesrvAddr("localhost:9876");
        consumer.setConsumerGroup("C-GROUP");
        consumer.setMessageModel(MessageModel.CLUSTERING);

        consumer.subscribe("ooxxA", "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                msgs.forEach(m -> {
                    System.out.println(m);
                    String.format("%s::%s::%s::%s::%s", m.getTopic(), m.getBrokerName(), m.getQueueId(), m.getTags(), new String(m.getBody()));
                });

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();

        System.in.read();

    }

}
