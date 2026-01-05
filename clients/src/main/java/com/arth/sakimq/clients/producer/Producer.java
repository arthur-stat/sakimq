package com.arth.sakimq.clients.producer;


import com.google.protobuf.ByteString;

import java.util.List;
import java.util.Map;

public interface Producer {

    Producer addBroker(String host, int port);

    /**
     * Remove a broker from the producer.
     *
     * @param host the broker host
     * @param port the broker port
     * @return this Producer instance
     */
    Producer removeBroker(String host, int port);

    void send(String topic, byte[] message) throws Exception;

    void send(List<String> topics, Map<String, String> headers, ByteString body);

    void start() throws Exception;

    void shutdown() throws Exception;
}