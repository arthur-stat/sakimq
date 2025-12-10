package com.arth.sakimq.network.netty;

import com.arth.sakimq.common.constant.LoggerName;
import com.arth.sakimq.network.config.NettyConfig;
import com.arth.sakimq.network.handler.ClientProtocolHandler;
import com.arth.sakimq.protocol.MessageType;
import com.arth.sakimq.protocol.TransportMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class NettyClient {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.NETTY_CLIENT);
    private final ClientProtocolHandler handler;
    private final NettyConfig config;
    private final Map<InetSocketAddress, Channel> brokerChannels = new ConcurrentHashMap<>();
    private final List<InetSocketAddress> brokerAddresses = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger roundRobinIndex = new AtomicInteger(0);
    private EventLoopGroup group;
    private SendStrategy sendStrategy = SendStrategy.ALL;

    /**
     * Default constructor for NettyClient.
     *
     * @param handler    the application protocol handler
     */
    public NettyClient(ClientProtocolHandler handler) {
        this.handler = handler;
        this.config = NettyConfig.getConfig();
    }

    /**
     * Constructor for NettyClient with custom config.
     *
     * @param handler    the application protocol handler
     * @param config     custom netty config
     */
    public NettyClient(ClientProtocolHandler handler, NettyConfig config) {
        this.handler = handler;
        this.config = config;
    }

    /**
     * Adds a broker to the client.
     *
     * @param host the broker host
     * @param port the broker port
     * @return this NettyClient for method chaining
     */
    public NettyClient addConnection(String host, int port) {
        InetSocketAddress address = new InetSocketAddress(host, port);
        synchronized (brokerAddresses) {
            if (!brokerAddresses.contains(address)) {
                brokerAddresses.add(address);
                log.debug("Added broker: {}", address);
            }
        }
        return this;
    }

    public NettyClient addConnection(String host) {
        InetSocketAddress address = new InetSocketAddress(host, config.getPort());
        synchronized (brokerAddresses) {
            if (!brokerAddresses.contains(address)) {
                brokerAddresses.add(address);
                log.debug("Added broker: {}", address);
            }
        }
        return this;
    }

    /**
     * Sets the send strategy.
     *
     * @param strategy the send strategy
     * @return this NettyClient for method chaining
     */
    public NettyClient setSendStrategy(SendStrategy strategy) {
        this.sendStrategy = strategy;
        log.debug("Set send strategy: {}", strategy);
        return this;
    }

    /**
     * Sets the send strategy by name.
     *
     * @param strategyName the strategy name ("all", "random", or "load_balance")
     * @return this NettyClient for method chaining
     */
    public NettyClient setSendStrategy(String strategyName) {
        this.sendStrategy = SendStrategy.valueOf(strategyName.toUpperCase());
        log.debug("Set send strategy: {}", this.sendStrategy);
        return this;
    }

    public void start() throws Exception {
        // Use virtual thread factory
        group = new MultiThreadIoEventLoopGroup(0, NioIoHandler.newFactory());

        // Connect to all brokers
        for (InetSocketAddress address : brokerAddresses) {
            connectToBroker(address);
        }
    }

    /**
     * Connects to a specific broker.
     *
     * @param address the broker address
     * @throws Exception if connection fails
     */
    private void connectToBroker(InetSocketAddress address) throws Exception {
        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(
                                new LengthFieldBasedFrameDecoder(
                                        config.getMaxFrameLength(),
                                        0,
                                        config.getLengthFieldLength(),
                                        0,
                                        config.getInitialBytesToStrip()),
                                new ProtobufDecoder(TransportMessage.getDefaultInstance()),
                                new LengthFieldPrepender(config.getLengthFieldLength()),
                                new ProtobufEncoder(),
                                new NettyClientHandler());
                    }
                });

        Channel channel = bootstrap.connect(address).sync().channel();
        brokerChannels.put(address, channel);
        log.debug("Netty client connected to {}", address);
    }

    public void shutdown() throws Exception {
        // Send DISCONNECT message to all brokers
        TransportMessage disconnectMsg = TransportMessage.newBuilder()
                .setType(MessageType.DISCONNECT)
                .setSeq(0)
                .setTimestamp(System.currentTimeMillis())
                .build();

        for (Map.Entry<InetSocketAddress, Channel> entry : brokerChannels.entrySet()) {
            Channel channel = entry.getValue();
            if (channel != null && channel.isOpen()) {
                try {
                    channel.writeAndFlush(disconnectMsg).sync();
                    log.debug("DISCONNECT message sent to {}", entry.getKey());
                } catch (Exception e) {
                    log.error("Failed to send DISCONNECT message to {}: ", entry.getKey(), e);
                }
                channel.close().sync();
            }
        }

        if (group != null && !group.isShuttingDown()) {
            group.shutdownGracefully().sync();
        }

        brokerChannels.clear();
        brokerAddresses.clear();
        log.debug("Netty client shutdown");
    }

    /**
     * Gets active channels.
     *
     * @return list of active channels
     */
    private List<Channel> getActiveChannels() {
        return brokerChannels.values().stream()
                .filter(Channel::isActive)
                .collect(Collectors.toList());
    }

    public CompletableFuture<Void> send(TransportMessage msg) {
        List<Channel> activeChannels = getActiveChannels();
        if (activeChannels.isEmpty()) {
            return CompletableFuture.failedFuture(new IllegalStateException("No active channels"));
        }

        switch (sendStrategy) {
            case ALL:
                return sendToAll(activeChannels, msg);
            case RANDOM:
                return sendToRandom(activeChannels, msg);
            case LOAD_BALANCE:
                // TODO
                return sendToRandom(activeChannels, msg);
            default:
                log.warn("Unknown send strategy: {}, falling back to all", sendStrategy);
                return sendToAll(activeChannels, msg);
        }
    }

    /**
     * Sends message to all active channels.
     *
     * @param channels list of active channels
     * @param msg      the message to send
     * @return CompletableFuture that completes when all sends are done
     */
    private CompletableFuture<Void> sendToAll(List<Channel> channels, TransportMessage msg) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (Channel channel : channels) {
            CompletableFuture<Void> future = sendToChannel(channel, msg);
            futures.add(future);
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenAccept(v -> log.debug("Message sent to all brokers, sequence: {}", msg.getSeq()));
    }

    /**
     * Sends message to a random active channel.
     *
     * @param channels list of active channels
     * @param msg      the message to send
     * @return CompletableFuture that completes when send is done
     */
    private CompletableFuture<Void> sendToRandom(List<Channel> channels, TransportMessage msg) {
        Channel channel = channels.get(new Random().nextInt(channels.size()));
        log.debug("Selected random channel to send message, sequence: {}", msg.getSeq());
        return sendToChannel(channel, msg);
    }

    /**
     * Sends message to a specific channel.
     *
     * @param channel the channel to send to
     * @param msg     the message to send
     * @return CompletableFuture that completes when send is done
     */
    private CompletableFuture<Void> sendToChannel(Channel channel, TransportMessage msg) {
        CompletableFuture<Void> future = new CompletableFuture<>();

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

    // Define send strategy types
    public enum SendStrategy {
        ALL,           // Send to all brokers
        RANDOM,        // Send to random broker
        LOAD_BALANCE   // Reserved for future load balancing implementation
    }

    /**
     * Netty client handler to dispatch incoming messages
     */
    private class NettyClientHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof TransportMessage transportMsg) {
                handler.dispatch(ctx, transportMsg);
            } else {
                log.error("Invalid message type: {}", msg.getClass().getName());
            }
        }

        // Send CONNECT message to server
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            log.info("TCP channel active");
            handler.onConnect(ctx);
            super.channelActive(ctx);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            log.info("TCP channel inactive");
            handler.onDisconnect(ctx);
            super.channelInactive(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            log.error("Netty error: ", cause);
            ctx.close();
        }
    }
}