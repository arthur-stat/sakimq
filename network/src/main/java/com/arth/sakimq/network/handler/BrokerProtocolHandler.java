package com.arth.sakimq.network.handler;

import com.arth.sakimq.protocol.TransportMessage;
import io.netty.channel.ChannelHandlerContext;

public interface BrokerProtocolHandler {

    void dispatch(ChannelHandlerContext ctx, TransportMessage msg);

    void onSendMessage(ChannelHandlerContext ctx, TransportMessage msg);

    void onHandleMessage(ChannelHandlerContext ctx, TransportMessage msg);

    void onSendAck(ChannelHandlerContext ctx, TransportMessage msg);

    void onHandleAck(ChannelHandlerContext ctx, TransportMessage msg);

    void onHeartbeat(ChannelHandlerContext ctx, TransportMessage msg);

    void onConnect(ChannelHandlerContext ctx, TransportMessage msg);

    void onDisconnect(ChannelHandlerContext ctx, TransportMessage msg);
}
