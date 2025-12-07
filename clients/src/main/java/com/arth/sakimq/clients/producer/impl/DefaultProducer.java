package com.arth.sakimq.clients.producer.impl;

import com.arth.sakimq.clients.producer.Producer;
import com.arth.sakimq.network.netty.NettyClient;
import com.arth.sakimq.protocol.Message;
import com.arth.sakimq.protocol.TransportMessage;
import com.arth.sakimq.common.utils.IdGenerator;

import java.util.UUID;

public class DefaultProducer implements Producer {

    private final String name;
    private final NettyClient client;

    public DefaultProducer(String host, int port) {
        name = "Producer-" + UUID.randomUUID();
        this.client = new NettyClient(host, port, name, new DefaultProducerTransportHandler());
    }

    public DefaultProducer(String host, int port, String name) {
        this.client = new NettyClient(host, port, name, new DefaultProducerTransportHandler());
        this.name = name;
    }

    @Override
    public void send(TransportMessage transportMessage) throws InterruptedException {
        client.send(transportMessage).join();
    }

    public void start() throws InterruptedException {
        client.connect().join();
    }

    @Override
    public void shutdown() {
        client.disconnect().join();
    }
}