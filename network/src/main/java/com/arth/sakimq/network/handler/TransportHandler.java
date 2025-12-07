package com.arth.sakimq.network.handler;

import com.arth.sakimq.protocol.TransportMessage;

public interface TransportHandler {

    void onMessage(TransportMessage msg);

    void onAck(TransportMessage msg);

    void onHeartbeat(TransportMessage msg);

    void onConnect(TransportMessage msg);

    void onDisconnect(TransportMessage msg);
}
