package com.arth.sakimq.test;

import com.arth.sakimq.broker.core.impl.DefaultBroker;
import com.arth.sakimq.clients.producer.impl.DefaultProducer;
import com.arth.sakimq.protocol.Message;
import com.arth.sakimq.protocol.MessagePack;
import com.arth.sakimq.protocol.MessageType;
import com.arth.sakimq.protocol.TransportMessage;
import com.google.protobuf.ByteString;

import java.util.List;
import java.util.Map;

public class ProducerToBroker {

    public static void main(String[] args) throws Exception {
        DefaultProducer producer = new DefaultProducer();
        producer.addBroker("localhost", 8080);

        DefaultBroker broker = new DefaultBroker(8080);

        broker.start();
        producer.start();

        for (int i = 0; i < 5; i++) {
            producer.send(List.of(), Map.of(), ByteString.copyFrom(new byte[0]));
            Thread.sleep(1000);
        }
    }

    public static TransportMessage testTransportMessage() {
        Message message = Message.newBuilder()
                .setMessageId(1001)
                .putHeaders("content-type", "application/json")
                .putHeaders("correlation-id", "test-correlation-123")
                .setBody(ByteString.copyFromUtf8("{\"test\": \"data\"}"))
                .setTimestamp(System.currentTimeMillis())
                .build();

        MessagePack messagePack = MessagePack.newBuilder()
                .addTopics("test.topic.1")
                .addTopics("test.topic.2")
                .setMessage(message)
                .build();

        return TransportMessage.newBuilder()
                .setType(MessageType.MESSAGE)
                .setSeq(12345)
                .setTimestamp(System.currentTimeMillis())
                .setMessagePack(messagePack)
                .build();
    }
}