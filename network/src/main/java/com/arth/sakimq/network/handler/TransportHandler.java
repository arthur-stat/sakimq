package com.arth.sakimq.network.handler;

import com.arth.sakimq.protocol.TransportProto;

public interface TransportHandler {

    void onMessage(TransportProto msg);

    void onAck(TransportProto msg);

    void onHeartbeat(TransportProto msg);

    void onConnect(TransportProto msg);

    void onDisconnect(TransportProto msg);
}
