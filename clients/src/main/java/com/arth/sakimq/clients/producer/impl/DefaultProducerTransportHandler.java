package com.arth.sakimq.clients.producer.impl;

import com.arth.sakimq.common.constant.LoggerName;
import com.arth.sakimq.network.handler.TransportHandler;
import com.arth.sakimq.protocol.TransportMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultProducerTransportHandler implements TransportHandler {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.PRODUCER);

    @Override
    public void onMessage(TransportMessage msg) {
        log.warn("Received message in producer handler, which is unexpected: {}", msg);
    }

    @Override
    public void onAck(TransportMessage msg) {

    }

    @Override
    public void onHeartbeat(TransportMessage msg) {

    }

    @Override
    public void onConnect(TransportMessage msg) {
        log.info("Connection established with server");
    }

    @Override
    public void onDisconnect(TransportMessage msg) {

    }
}