package com.arth.sakimq.network.handler;

import com.arth.sakimq.protocol.TransportMessage;
import io.netty.channel.Channel;

public interface TransportHandler {

    void onMessage(Channel channel, TransportMessage msg);

    void onAck(Channel channel, TransportMessage msg);

    void onHeartbeat(Channel channel, TransportMessage msg);

    void onConnect(Channel channel, TransportMessage msg);

    void onDisconnect(Channel channel, TransportMessage msg);
}
