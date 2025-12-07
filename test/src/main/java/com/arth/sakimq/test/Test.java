package com.arth.sakimq.test;

import com.arth.sakimq.broker.core.impl.DefaultBroker;
import com.arth.sakimq.clients.producer.Producer;
import com.arth.sakimq.clients.producer.impl.DefaultProducer;
import com.arth.sakimq.broker.topic.TopicsManager;
import com.arth.sakimq.protocol.Message;
import com.arth.sakimq.protocol.MessagePack;
import com.arth.sakimq.protocol.MessageType;
import com.arth.sakimq.protocol.TransportMessage;

public class Test {
    public static void main(String[] args) throws InterruptedException {
        Producer producer = new DefaultProducer("localhost", 8080);
        DefaultBroker broker = new DefaultBroker(8080);

        broker.start();
        producer.start();

        producer.send(testTransportMessage());

    }

    public static TransportMessage testTransportMessage() {
        Message message = Message.newBuilder()
                .setMessageId(1001)
                .putHeaders("content-type", "application/json")
                .putHeaders("correlation-id", "test-correlation-123")
                .setBody(com.google.protobuf.ByteString.copyFromUtf8("{\"test\": \"data\"}"))
                .setTimestamp(System.currentTimeMillis())
                .build();

        MessagePack messagePack = MessagePack.newBuilder()
                .addTopics("test.topic.1")
                .addTopics("test.topic.2")
                .setMessage(message)
                .build();

        return TransportMessage.newBuilder()
                .setType(MessageType.MESSAGE)
                .setDeliveryTag(12345)
                .setTimestamp(System.currentTimeMillis())
                .setMessagePack(messagePack)
                .build();
    }
}