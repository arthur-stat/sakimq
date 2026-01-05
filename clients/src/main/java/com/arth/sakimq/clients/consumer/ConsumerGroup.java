package com.arth.sakimq.clients.consumer;

import com.arth.sakimq.protocol.MessagePack;

import java.util.List;
import java.util.function.Consumer;

public interface ConsumerGroup {

    ConsumerGroup subscribe(List<String> topics);

    ConsumerGroup onMessage(Consumer<MessagePack> listener);

    MessagePack pollLocal();

    ConsumerGroup addBroker(String host, int port);

    /**
     * Remove a broker from the consumer group.
     *
     * @param host the broker host
     * @param port the broker port
     * @return this ConsumerGroup instance
     */
    ConsumerGroup removeBroker(String host, int port);

    /**
     * Starts the consumer group.
     */
    void start();

    void shutdown();

}