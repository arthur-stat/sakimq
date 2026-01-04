package com.arth.sakimq.clients.consumer;

public interface ConsumerGroup {

    ConsumerGroup addBroker(String host, int port);

    void start();

    void shutdown();

}