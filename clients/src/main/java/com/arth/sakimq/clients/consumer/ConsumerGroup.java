package com.arth.sakimq.clients.consumer;

import com.arth.sakimq.protocol.MessagePack;

import java.util.List;
import java.util.function.Consumer;

public interface ConsumerGroup {

    ConsumerGroup subscribe(List<String> topics);

    ConsumerGroup onMessage(Consumer<MessagePack> listener);

    MessagePack pollLocal();

    ConsumerGroup addBroker(String host, int port);

    void start();

    void shutdown();

}