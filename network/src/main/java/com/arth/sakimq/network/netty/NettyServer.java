package com.arth.sakimq.network.netty;

import com.arth.sakimq.common.constant.LoggerName;
import com.arth.sakimq.network.config.NettyConfig;
import com.arth.sakimq.network.handler.BrokerProtocolHandler;
import com.arth.sakimq.network.handler.ClientProtocolHandler;
import com.arth.sakimq.protocol.MessageType;
import com.arth.sakimq.protocol.TransportMessage;
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

public class NettyServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.NETTY_SERVER);
    private final int port;
    private Channel serverChannel;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private final BrokerProtocolHandler handler;
    private final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    private final NettyConfig config;

    public NettyServer(BrokerProtocolHandler handler) {
        this(NettyConfig.getConfig(), handler);
    }

    public NettyServer(int port, BrokerProtocolHandler handler) {
        this.config = NettyConfig.getConfig();
        this.port = port;
        this.handler = handler;
    }

    public NettyServer(NettyConfig config, BrokerProtocolHandler handler) {
        this.config = config;
        this.port = config.getPort();
        this.handler = handler;
    }

    public CompletableFuture<Void> start() throws InterruptedException {
        CompletableFuture<Void> startFuture = new CompletableFuture<>();

        bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        workerGroup = new MultiThreadIoEventLoopGroup(0, NioIoHandler.newFactory());

        log.info("NettyServer using configuration: port={}, timeout={}, maxFrameLength={}, lengthFieldLength={}, initialBytesToStrip={}",
                config.getPort(), config.getTimeout(), config.getMaxFrameLength(),
                config.getLengthFieldLength(), config.getInitialBytesToStrip());

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

                        // Use config values for LengthFieldBasedFrameDecoder
                        pipeline.addLast("lenDecoder", new LengthFieldBasedFrameDecoder(
                                config.getMaxFrameLength(), 0,
                                config.getLengthFieldLength(), 0,
                                config.getInitialBytesToStrip()));
                        pipeline.addLast("protoDecoder", new ProtobufDecoder(TransportMessage.getDefaultInstance()));

                        pipeline.addLast("lenEncoder", new LengthFieldPrepender(config.getLengthFieldLength()));
                        pipeline.addLast("protoEncoder", new ProtobufEncoder());

                        pipeline.addLast("serverHandler", new SimpleChannelInboundHandler<TransportMessage>() {

                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, TransportMessage msg) throws Exception {
                                handler.dispatch(ctx, msg);
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                Channel channel = ctx.channel();
                                channels.add(channel);
                                log.info("Client connected: {}", channel.remoteAddress());
                                // Don't automatically create CONNECT message
                                // Wait for client to send its own CONNECT message
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                Channel channel = ctx.channel();
                                channels.remove(channel);
                                log.info("Client disconnected: {}", channel.remoteAddress());
                                // Don't automatically create DISCONNECT message
                                // Wait for client to send its own DISCONNECT message or handle connection cleanup
                                // Call handler's onDisconnect method with null to indicate connection loss
                                handler.onDisconnect(ctx, null);
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

    public CompletableFuture<Void> send(Channel channel, TransportMessage msg) {
        if (channel == null || !channel.isActive()) {
            CompletableFuture<Void> failed = new CompletableFuture<>();
            failed.completeExceptionally(new RuntimeException("Target channel not active"));
            return failed;
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        ChannelFuture writeFuture = channel.writeAndFlush(msg);
        writeFuture.addListener((ChannelFutureListener) wf -> {
            if (wf.isSuccess()) {
                future.complete(null);
            } else {
                future.completeExceptionally(wf.cause());
            }
        });

        return future;
    }

    public CompletableFuture<Void> broadcast(TransportMessage msg) {
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

    public int getPort() {
        return port;
    }

    @Override
    public void close() {
        shutdown().join();
    }
}