package com.arth.sakimq.clients.producer.impl;

import com.arth.sakimq.common.constant.LoggerName;
import com.arth.sakimq.network.handler.TransportHandler;
import com.arth.sakimq.protocol.TransportProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerTransportHandler implements TransportHandler {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.PRODUCER);

    @Override
    public void onMessage(TransportProto msg) {
        log.warn("Received message in producer handler, which is unexpected: {}", msg);
    }

    @Override
    public void onAck(TransportProto msg) {

    }

    @Override
    public void onHeartbeat(TransportProto msg) {

    }

    @Override
    public void onConnect(TransportProto msg) {

    }

    @Override
    public void onDisconnect(TransportProto msg) {

    }
}
