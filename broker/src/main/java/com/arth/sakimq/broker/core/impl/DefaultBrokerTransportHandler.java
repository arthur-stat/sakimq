package com.arth.sakimq.broker.core.impl;

import com.arth.sakimq.broker.session.SessionManager;
import com.arth.sakimq.common.constant.LoggerName;
import com.arth.sakimq.broker.topic.TopicsManager;
import com.arth.sakimq.network.handler.TransportHandler;
import com.arth.sakimq.protocol.TransportMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultBrokerTransportHandler implements TransportHandler {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER);
    private final TopicsManager topicsManager;
    private final SessionManager sessionManager;

    public DefaultBrokerTransportHandler(TopicsManager topicsManager, SessionManager sessionManager) {
        this.topicsManager = topicsManager;
        this.sessionManager = sessionManager;
    }

    @Override
    public void onMessage(TransportMessage msg) {
        topicsManager.publish(msg.getMessagePack());
    }

    @Override
    public void onAck(TransportMessage msg) {

    }

    @Override
    public void onHeartbeat(TransportMessage msg) {

    }

    @Override
    public void onConnect(TransportMessage msg) {
        log.info("Connection established with client {}", msg.getConnect().getClientId());
    }

    @Override
    public void onDisconnect(TransportMessage msg) {

    }
}