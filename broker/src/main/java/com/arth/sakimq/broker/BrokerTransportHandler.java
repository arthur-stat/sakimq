package com.arth.sakimq.broker;

import com.arth.sakimq.network.handler.TransportHandler;
import com.arth.sakimq.protocol.TransportMessage;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BrokerTransportHandler implements TransportHandler {

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    @Override
    public void onMessage(TransportMessage msg) {

    }

    @Override
    public void onAck(TransportMessage msg) {

    }

    @Override
    public void onHeartbeat(TransportMessage msg) {

    }

    @Override
    public void onConnect(TransportMessage msg) {

    }

    @Override
    public void onDisconnect(TransportMessage msg) {

    }
}
