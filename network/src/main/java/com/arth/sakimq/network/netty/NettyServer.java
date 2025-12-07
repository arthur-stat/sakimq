package com.arth.sakimq.network.netty;

import com.arth.sakimq.common.constant.LoggerName;
import com.arth.sakimq.network.config.NettyClientConfig;
import com.arth.sakimq.network.handler.TransportHandler;
import com.arth.sakimq.protocol.TransportProto;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class NettyServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.NETTY_SERVER);
    private final int port;
    private Channel serverChannel;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private final TransportHandler handler;
    private final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    private final ConcurrentMap<Integer, CompletableFuture<Void>> pendingAcks = new ConcurrentHashMap<>();

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
                                        case ACK -> {
                                            CompletableFuture<Void> f = pendingAcks.remove(msg.getDeliveryTag());
                                            if (f != null) f.complete(null);
                                            handler.onAck(msg);
                                        }
                                        case MESSAGE -> handler.onMessage(msg);
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
                                channels.add(ctx.channel());
                                log.info("Client connected: {}", ctx.channel().remoteAddress());
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                channels.remove(ctx.channel());
                                log.info("Client disconnected: {}", ctx.channel().remoteAddress());
                                pendingAcks.forEach((k, f) -> f.completeExceptionally(new RuntimeException("Client disconnected")));
                                pendingAcks.clear();
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

    public CompletableFuture<Void> send(Channel channel, TransportProto msg) {
        return attemptSend(channel, msg, NettyClientConfig.maxRetries);
    }

    private CompletableFuture<Void> attemptSend(Channel channel, TransportProto msg, int remainingAttempts) {
        if (channel == null || !channel.isActive()) {
            CompletableFuture<Void> failed = new CompletableFuture<>();
            failed.completeExceptionally(new RuntimeException("Target channel not active"));
            return failed;
        }

        int tag = msg.getDeliveryTag();
        CompletableFuture<Void> ackFuture = new CompletableFuture<>();
        pendingAcks.put(tag, ackFuture);

        ChannelFuture writeFuture = channel.writeAndFlush(msg);
        writeFuture.addListener((ChannelFutureListener) wf -> {
            if (!wf.isSuccess()) {
                CompletableFuture<Void> removed = pendingAcks.remove(tag);
                if (removed != null) removed.completeExceptionally(wf.cause());
            }
        });

        CompletableFuture<Void> timeoutFuture = ackFuture.orTimeout(NettyClientConfig.timeout, TimeUnit.SECONDS);

        return timeoutFuture.handle((res, ex) -> {
            if (ex == null) {
                return CompletableFuture.<Void>completedFuture(null);
            } else {
                pendingAcks.remove(tag);
                if (remainingAttempts > 1) {
                    log.warn("Send to {} failed, retrying (remaining {}): {}", channel.remoteAddress(), remainingAttempts - 1, ex.getMessage());
                    return attemptSend(channel, msg, remainingAttempts - 1);
                } else {
                    CompletableFuture<Void> failed = new CompletableFuture<>();
                    failed.completeExceptionally(ex);
                    return failed;
                }
            }
        }).thenCompose(Function.identity());
    }

    public CompletableFuture<Void> broadcast(TransportProto msg) {
        CompletableFuture<Void> all = CompletableFuture.completedFuture(null);
        for (Channel ch : channels) {
            all = all.thenCompose(v -> send(ch, msg).exceptionallyCompose(ex -> {
                CompletableFuture<Void> failed = new CompletableFuture<>();
                failed.completeExceptionally(ex);
                return failed;
            }));
        }
        return all;
    }

    public CompletableFuture<Void> shutdown() {
        CompletableFuture<Void> stopFuture = new CompletableFuture<>();

        if (serverChannel == null) {
            cleanup();
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

    public ChannelGroup getChannels() {
        return channels;
    }

    public Channel getServerChannel() {
        return serverChannel;
    }

    @Override
    public void close() {
        shutdown().join();
    }
}