package com.arth.sakimq.network.netty;

import com.arth.sakimq.common.constant.LoggerName;
import com.arth.sakimq.network.config.NettyClientConfig;
import com.arth.sakimq.network.handler.TransportHandler;
import com.arth.sakimq.protocol.MessageType;
import com.arth.sakimq.protocol.TransportMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class NettyClient {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.NETTY_CLIENT);
    private final String host;
    private final int port;
    private final String clientName;
    private final TransportHandler handler;
    private final NettyClientConfig config;
    private EventLoopGroup group;
    private Channel channel;

    public NettyClient(String host, int port, String clientName, TransportHandler handler, NettyClientConfig config) {
        this.host = host;
        this.port = port;
        this.clientName = clientName;
        this.handler = handler;
        this.config = config;
    }

    public void start() throws Exception {
        group = new NioEventLoopGroup();
        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(
                                new LengthFieldBasedFrameDecoder(64 * 1024 * 1024, 0, 4, 0, 4), // 64MB limit
                                new ProtobufDecoder(TransportMessage.getDefaultInstance()),
                                new LengthFieldPrepender(4),
                                new ProtobufEncoder(),
                                new NettyClientHandler());
                    }
                });

        channel = bootstrap.connect(host, port).sync().channel();
        log.debug("Netty client connected to {}:{}", host, port);
    }

    public void shutdown() throws Exception {
        if (channel != null && channel.isOpen()) {
            channel.close().sync();
        }
        if (group != null && !group.isShuttingDown()) {
            group.shutdownGracefully().sync();
        }
        log.debug("Netty client shutdown");
    }

    public CompletableFuture<Void> send(TransportMessage msg) {
        if (channel == null || !channel.isActive()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Channel is not active"));
        }

        CompletableFuture<Void> future = new CompletableFuture<>();

        // Send the message
        channel.writeAndFlush(msg).addListener((ChannelFutureListener) cf -> {
            if (cf.isSuccess()) {
                log.debug("Message sent successfully, sequence: {}", msg.getSeq());
                future.complete(null);
            } else {
                log.error("Failed to send message, sequence: {}", msg.getSeq(), cf.cause());
                future.completeExceptionally(cf.cause());
            }
        });

        return future;
    }

    /**
     * Netty client handler to handle incoming messages
     */
    private class NettyClientHandler extends ChannelInboundHandlerAdapter {

        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof TransportMessage transportMsg) {
                Channel channel = ctx.channel();
                switch (transportMsg.getType()) {
                    case ACK -> handler.onAck(channel, transportMsg);
                    case MESSAGE -> handler.onMessage(channel, transportMsg);
                    case HEARTBEAT -> handler.onHeartbeat(channel, transportMsg);
                    case CONNECT -> handler.onConnect(channel, transportMsg);
                    case DISCONNECT -> handler.onDisconnect(channel, transportMsg);
                    default -> log.warn("Received unknown message type: {}", transportMsg.getType());
                }
            }
        }

        // Send CONNECT message to server
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            log.info("TCP channel active");
            Channel channel = ctx.channel();
            handler.onConnect(channel, null);
            super.channelActive(ctx);
        }

        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            log.info("TCP channel inactive");
            super.channelInactive(ctx);
        }

        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            log.error("Netty error: ", cause);
            ctx.close();
        }
    }
}