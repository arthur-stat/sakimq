package com.arth.sakimq.network.netty;

import com.arth.sakimq.network.handler.TransportHandler;
import com.arth.sakimq.protocol.TransportProto;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.arth.sakimq.common.constant.LoggerName;

import java.util.concurrent.CompletableFuture;

public class NettyServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.NETTY_SERVER);
    private final int port;
    private Channel serverChannel;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private final TransportHandler handler;

    public NettyServer(int port, TransportHandler handler) {
        this.port = port;
        this.handler = handler;
    }

    public CompletableFuture<Void> start() throws InterruptedException {
        CompletableFuture<Void> startFuture = new CompletableFuture<>();

        bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        workerGroup = new MultiThreadIoEventLoopGroup(
                0,
                NioIoHandler.newFactory()
        );

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();

                        pipeline.addLast("lenDecoder", new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4));
                        pipeline.addLast("protoDecoder", new ProtobufDecoder(TransportProto.getDefaultInstance()));

                        pipeline.addLast("lenEncoder", new LengthFieldPrepender(4));
                        pipeline.addLast("protoEncoder", new ProtobufEncoder());

                        pipeline.addLast("serverHandler", new SimpleChannelInboundHandler<TransportProto>() {

                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, TransportProto msg) throws Exception {
                                try {
                                    switch (msg.getType()) {
                                        case MESSAGE -> handler.onMessage(msg);
                                        case ACK -> handler.onAck(msg);
                                        case HEARTBEAT -> handler.onHeartbeat(msg);
                                        case CONNECT -> handler.onConnect(msg);
                                        case DISCONNECT -> handler.onDisconnect(msg);
                                        default -> log.warn("Received unknown message type: {}", msg.getType());
                                    }
                                } catch (Exception e) {
                                    log.error("Error handling message: ", e);
                                }
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                log.info("Client connected: {}", ctx.channel().remoteAddress());
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                log.info("Client disconnected: {}", ctx.channel().remoteAddress());
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                                log.error("Exception caught: ", cause);
                                ctx.close();
                            }
                        });
                    }
                });

        ChannelFuture bindFuture = bootstrap.bind(port);
        bindFuture.addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                serverChannel = future.channel();
                log.info("Netty server started on port {}", port);
                startFuture.complete(null);
            } else {
                cleanup();
                startFuture.completeExceptionally(future.cause());
            }
        });

        return startFuture;
    }

    public CompletableFuture<Void> shutdown() {
        CompletableFuture<Void> stopFuture = new CompletableFuture<>();

        if (serverChannel == null) {
            stopFuture.complete(null);
            return stopFuture;
        }

        serverChannel.close().addListener(future -> {
            cleanup();
            if (future.isSuccess()) stopFuture.complete(null);
            else stopFuture.completeExceptionally(future.cause());
        });

        return stopFuture;
    }

    private void cleanup() {
        if (bossGroup != null) bossGroup.shutdownGracefully();
        if (workerGroup != null) workerGroup.shutdownGracefully();
    }

    public Channel getServerChannel() {
        return serverChannel;
    }

    @Override
    public void close() {
        shutdown().join();
    }
}