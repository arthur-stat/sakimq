package com.arth.sakimq.network.netty;

import com.arth.sakimq.common.constant.LoggerName;
import com.arth.sakimq.common.exception.UnavailableChannelException;
import com.arth.sakimq.network.config.NettyClientConfig;
import com.arth.sakimq.network.handler.TransportHandler;
import com.arth.sakimq.protocol.TransportProto;
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
                        pipeline.addLast("protoDecoder", new ProtobufDecoder(TransportProto.getDefaultInstance()));

                        pipeline.addLast("clientHandler", new SimpleChannelInboundHandler<TransportProto>() {

                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, TransportProto msg) {
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
                            public void channelActive(ChannelHandlerContext ctx) {
                                log.info("Connected to server: {}", ctx.channel().remoteAddress());
                                clientChannel = ctx.channel();
                                connectionFuture.complete(null);
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) {
                                log.info("Disconnected from server: {}", ctx.channel().remoteAddress());
                                connectionFuture.completeExceptionally(new RuntimeException("Connection lost"));
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

    public void send(TransportProto msg) throws Exception {
        Exception lastException = null;

        for (int attempt = 0; attempt < NettyClientConfig.maxRetries; attempt++) {
            try {
                sendWithoutRetry(msg);
                return;
            } catch (Exception e) {
                lastException = e;
                if (attempt < NettyClientConfig.maxRetries - 1) {
                    log.warn("Send failed (attempt {} of {}): ", attempt + 1, NettyClientConfig.maxRetries, e);
                }
            }
        }

        log.error("Send failed after {} attempts", NettyClientConfig.maxRetries, lastException);
        throw lastException;
    }

    private void sendWithoutRetry(TransportProto msg) throws Exception {
        if (clientChannel == null || !clientChannel.isActive()) {
            throw new UnavailableChannelException("Not connected");
        }

        int tag = msg.getDeliveryTag();
        CompletableFuture<Void> ackFuture = new CompletableFuture<>();
        pendingAcks.put(tag, ackFuture);

        try {
            /* async send messages */
            clientChannel.writeAndFlush(msg);
            /* sync wait-and-get in a virtual thread: guaranteed by the upper-layer caller */
            ackFuture.get(NettyClientConfig.timeout, TimeUnit.SECONDS);
        } catch (Exception e) {
            pendingAcks.remove(tag);
            throw e;
        }
    }

    public void disconnect() {
        if (clientChannel != null) clientChannel.close();
        if (workerGroup != null) workerGroup.shutdownGracefully();
    }

    @Override
    public void close() {
        disconnect();
    }
}