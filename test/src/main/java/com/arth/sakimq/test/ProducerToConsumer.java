package com.arth.sakimq.test;

import com.arth.sakimq.broker.core.impl.DefaultBroker;
import com.arth.sakimq.clients.consumer.ConsumerGroup;
import com.arth.sakimq.clients.consumer.impl.SingleConsumer;
import com.arth.sakimq.clients.producer.Producer;
import com.arth.sakimq.clients.producer.impl.DefaultProducer;
import com.google.protobuf.ByteString;
import java.util.concurrent.CompletableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple end-to-end sanity test: Producer -> Broker -> Consumer with topics.
 */
public class ProducerToConsumer {

    public static void main(String[] args) throws Exception {
        int port = 8082;
        String topicA = "topic.alpha";
        String topicB = "topic.beta";

        System.out.println("Starting ProducerToConsumer test...");
        DefaultBroker broker = new DefaultBroker(port);
        System.out.println("Starting Broker on port " + port);
        broker.start();

        // Prepare consumers
        System.out.println("Setting up consumers...");
        List<ConsumerGroup> consumers = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(12);  // 3 for A, 3 for B, 6 for both
        AtomicInteger c1Count = new AtomicInteger();
        AtomicInteger c2Count = new AtomicInteger();
        AtomicInteger c3Count = new AtomicInteger();

        ConsumerGroup c1 = new SingleConsumer("Consumer-A")
                .subscribe(List.of(topicA))
                .addBroker("localhost", port)
                .onMessage(msg -> {
                    System.out.println("Consumer-A received message on " + msg.getTopicsList());
                    c1Count.incrementAndGet();
                    latch.countDown();
                });
        ConsumerGroup c2 = new SingleConsumer("Consumer-B")
                .subscribe(List.of(topicB))
                .addBroker("localhost", port)
                .onMessage(msg -> {
                    System.out.println("Consumer-B received message on " + msg.getTopicsList());
                    c2Count.incrementAndGet();
                    latch.countDown();
                });
        ConsumerGroup c3 = new SingleConsumer("Consumer-All")
                .subscribe(List.of(topicA, topicB))
                .addBroker("localhost", port)
                .onMessage(msg -> {
                    System.out.println("Consumer-All received message on " + msg.getTopicsList());
                    c3Count.incrementAndGet();
                    latch.countDown();
                });

        consumers.add(c1);
        consumers.add(c2);
        consumers.add(c3);

        consumers.forEach(ConsumerGroup::start);

        // Prepare producers
        System.out.println("Setting up producers...");
        Producer p1 = new DefaultProducer("Producer-A").addBroker("localhost", port);
        Producer p2 = new DefaultProducer("Producer-B").addBroker("localhost", port);
        Producer p3 = new DefaultProducer("Producer-AB").addBroker("localhost", port);

        p1.start();
        p2.start();
        p3.start();

        // Send messages to different topics
        System.out.println("Sending messages...");
        sendMany(p1, topicA, 3);
        sendMany(p2, topicB, 3);
        sendMany(p3, List.of(topicA, topicB), 3);

        // Wait for delivery
        boolean allReceived = latch.await(15, TimeUnit.SECONDS);

        System.out.printf("C1 (A) received: %d%n", c1Count.get());
        System.out.printf("C2 (B) received: %d%n", c2Count.get());
        System.out.printf("C3 (A+B) received: %d%n", c3Count.get());
        System.out.printf("All expected received: %s%n", allReceived);

        // Cleanup
        consumers.forEach(ConsumerGroup::shutdown);
        p1.shutdown();
        p2.shutdown();
        p3.shutdown();
        broker.shutdown();
    }

    private static void sendMany(Producer producer, String topic, int count) {
        sendMany(producer, List.of(topic), count);
    }

    private static void sendMany(Producer producer, List<String> topics, int count) {
        for (int i = 0; i < count; i++) {
            try {
                producer.send(topics, Map.of("index", String.valueOf(i)), ByteString.copyFromUtf8("payload-" + i));
                System.out.println("Message sent successfully, sequence: " + i);
            } catch (Exception e) {
                System.err.println("Failed to send message sequence: " + i + ", error: " + e.getMessage());
            }
        }
    }
}
