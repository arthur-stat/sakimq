package com.arth.sakimq.clients.producer;


import com.arth.sakimq.protocol.TransportMessage;
import com.google.protobuf.ByteString;

import java.util.List;
import java.util.Map;

public interface Producer {

    Producer addBroker(String host, int port);

    void send(List<String> topics, Map<String, String> headers, ByteString body);

    void start() throws Exception;

    void shutdown() throws Exception;
}