package com.arth.sakimq.network.netty;

import com.arth.sakimq.common.constant.LoggerName;
import com.arth.sakimq.common.exception.UnavailableChannelException;
import com.arth.sakimq.network.config.NettyClientConfig;
import com.arth.sakimq.network.handler.TransportHandler;
import com.arth.sakimq.protocol.TransportMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class NettyClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.NETTY_CLIENT);
    private final String host;
    private final int port;
    private final TransportHandler handler;
    private Channel clientChannel;
    private EventLoopGroup workerGroup;
    private CompletableFuture<Void> connectionFuture = new CompletableFuture<>();
    private final ConcurrentMap<Integer, CompletableFuture<Void>> pendingAcks = new ConcurrentHashMap<>();

    public NettyClient(String host, int port, TransportHandler handler) {
        this.host = host;
        this.port = port;
        this.handler = handler;
    }

    public CompletableFuture<Void> connect() throws InterruptedException {
        workerGroup = new MultiThreadIoEventLoopGroup(
                0,
                NioIoHandler.newFactory()
        );

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();

                        pipeline.addLast("lenEncoder", new LengthFieldPrepender(4));
                        pipeline.addLast("protoEncoder", new ProtobufEncoder());

                        pipeline.addLast("lenDecoder", new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4));
                        pipeline.addLast("protoDecoder", new ProtobufDecoder(TransportMessage.getDefaultInstance()));

                        pipeline.addLast("clientHandler", new SimpleChannelInboundHandler<TransportMessage>() {

                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, TransportMessage msg) {
                                try {
                                    switch (msg.getType()) {
                                        case MESSAGE -> handler.onMessage(msg);
                                        case ACK -> {
                                            handler.onAck(msg);
                                            CompletableFuture<Void> f = pendingAcks.remove(msg.getDeliveryTag());
                                            if (f != null) f.complete(null);
                                        }
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
                            public void channelActive(ChannelHandlerContext ctx) {
                                log.info("Connected to server: {}", ctx.channel().remoteAddress());
                                clientChannel = ctx.channel();
                                connectionFuture.complete(null);
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) {
                                log.info("Disconnected from server: {}", ctx.channel().remoteAddress());
                                connectionFuture.completeExceptionally(new RuntimeException("Connection lost"));
                                pendingAcks.forEach((k, f) -> f.completeExceptionally(new RuntimeException("Channel closed")));
                                pendingAcks.clear();
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                log.error("Exception caught: ", cause);
                                ctx.close();
                            }
                        });
                    }
                });

        ChannelFuture connectFuture = bootstrap.connect(host, port);
        connectFuture.addListener(future -> {
            if (future.isSuccess()) {
                clientChannel = connectFuture.channel();
                connectionFuture.complete(null);
            } else {
                connectionFuture.completeExceptionally(future.cause());
            }
        });

        return connectionFuture;
    }

    public CompletableFuture<Void> send(TransportMessage msg) {
        return attemptSend(msg, NettyClientConfig.maxRetries);
    }

    private CompletableFuture<Void> attemptSend(TransportMessage msg, int remainingAttempts) {
        if (clientChannel == null || !clientChannel.isActive()) {
            CompletableFuture<Void> failed = new CompletableFuture<>();
            failed.completeExceptionally(new UnavailableChannelException("Not connected"));
            return failed;
        }

        int tag = msg.getDeliveryTag();
        CompletableFuture<Void> ackFuture = new CompletableFuture<>();
        pendingAcks.put(tag, ackFuture);

        /* async send messages */
        ChannelFuture writeFuture = clientChannel.writeAndFlush(msg);
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
                    log.warn("Send failed, retrying (remaining {}): {}", remainingAttempts - 1, ex.getMessage());
                    return attemptSend(msg, remainingAttempts - 1);
                } else {
                    CompletableFuture<Void> failed = new CompletableFuture<>();
                    failed.completeExceptionally(ex);
                    return failed;
                }
            }
        }).thenCompose(Function.identity());
    }

    public CompletableFuture<Void> disconnect() {
        CompletableFuture<Void> disconnectFuture = new CompletableFuture<>();
        if (clientChannel != null) {
            clientChannel.close().addListener(cf -> {
                if (cf.isSuccess()) disconnectFuture.complete(null);
                else disconnectFuture.completeExceptionally(cf.cause());
            });
        } else {
            disconnectFuture.complete(null);
        }
        if (workerGroup != null) workerGroup.shutdownGracefully();
        return disconnectFuture;
    }

    @Override
    public void close() {
        disconnect().join();
    }
}