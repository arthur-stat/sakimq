package com.arth.sakimq.test;

import com.arth.sakimq.broker.core.impl.DefaultBroker;
import com.arth.sakimq.clients.consumer.impl.SingleConsumer;
import com.arth.sakimq.clients.producer.impl.DefaultProducer;
import com.google.protobuf.ByteString;

import java.util.List;
import java.util.Map;

public class BrokerToConsumer {

    public static void main(String[] args) throws Exception {
        int port = 8081;

        DefaultBroker broker = new DefaultBroker(port);
        DefaultProducer producer = new DefaultProducer();
        SingleConsumer consumer = new SingleConsumer();

        broker.start().join();

        producer.addBroker("localhost", port);
        SingleConsumer c = (SingleConsumer) consumer.addBroker("localhost", port);
        c.onMessage(msg -> System.out.println("Consumer received message: " + msg.getMessage().getMessageId()));

        consumer.start();
        producer.start();

        for (int i = 0; i < 3; i++) {
            producer.send(List.of("demo"), Map.of("index", String.valueOf(i)),
                    ByteString.copyFromUtf8("hello-" + i));
        }

        Thread.sleep(2000);

        consumer.shutdown();
        producer.shutdown();
        broker.shutdown().join();
    }
}
