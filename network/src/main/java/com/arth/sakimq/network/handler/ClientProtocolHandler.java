package com.arth.sakimq.network.handler;

import com.arth.sakimq.protocol.TransportMessage;
import io.netty.channel.ChannelHandlerContext;

public interface ClientProtocolHandler {

    void dispatch(ChannelHandlerContext ctx, TransportMessage msg);

    void onMessage(ChannelHandlerContext ctx, TransportMessage msg);

    void onAck(ChannelHandlerContext ctx, TransportMessage msg);

    void onHeartbeat(ChannelHandlerContext ctx, TransportMessage msg);

    void onConnect(ChannelHandlerContext ctx);

    void onDisconnect(ChannelHandlerContext ctx);
}