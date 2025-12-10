package com.arth.sakimq.clients.producer.impl;

import com.arth.sakimq.clients.config.ProducerConfig;
import com.arth.sakimq.clients.producer.Producer;
import com.arth.sakimq.common.exception.UnavailableChannelException;
import com.arth.sakimq.network.handler.ClientProtocolHandler;
import com.arth.sakimq.network.netty.NettyClient;
import com.arth.sakimq.network.config.NettyConfig;
import com.arth.sakimq.protocol.Message;
import com.arth.sakimq.protocol.MessagePack;
import com.arth.sakimq.protocol.MessageType;
import com.arth.sakimq.protocol.TransportMessage;
import com.google.protobuf.ByteString;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultProducer implements Producer, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DefaultProducer.class);
    private final String name;
    private final NettyClient client;
    private final ProducerConfig config;
    private final AtomicLong seq = new AtomicLong(0);
    private final ConcurrentMap<Long, CompletableFuture<Void>> pendingAcks = new ConcurrentHashMap<>();

    public DefaultProducer() {
        this("Producer-" + UUID.randomUUID());
    }

    public DefaultProducer(String name) {
        this(name, ProducerConfig.getConfig(), NettyConfig.getConfig());
    }

    public DefaultProducer(String name, ProducerConfig producerConfig, NettyConfig nettyConfig) {
        this.name = name;
        this.client = new NettyClient(new DefaultProducerHandler(), nettyConfig);
        this.config = producerConfig;
    }

    @Override
    public Producer addBroker(String host, int port) {
        client.addConnection(host, port);
        return this;
    }

    @Override
    public void send(List<String> topics, Map<String, String> headers, ByteString body) {
        long messageId = seq.incrementAndGet();

        Message message = Message.newBuilder()
                .setMessageId(messageId)
                .putAllHeaders(headers)
                .setBody(body)
                .setTimestamp(System.currentTimeMillis())
                .build();

        MessagePack messagePack = MessagePack.newBuilder()
                .addAllTopics(topics)
                .setMessage(message)
                .build();

        TransportMessage transportMessage = TransportMessage.newBuilder()
                .setType(MessageType.MESSAGE)
                .setSeq(messageId)
                .setTimestamp(System.currentTimeMillis())
                .setMessagePack(messagePack)
                .build();

        attemptSend(transportMessage, config.getMaxRetries()).join();
    }

    private CompletableFuture<Void> attemptSend(TransportMessage msg, int remainingAttempts) {
        long deliveryTag = msg.getSeq();
        CompletableFuture<Void> ackFuture = new CompletableFuture<>();
        pendingAcks.put(deliveryTag, ackFuture);

        return client.send(msg)
                .thenCompose(v -> ackFuture.orTimeout(config.getTimeout(), TimeUnit.MILLISECONDS))
                .exceptionallyCompose(e -> {
                    pendingAcks.remove(deliveryTag);
                    if (remainingAttempts > 1) {
                        log.warn("Send failed, retrying (remaining {}): {}", remainingAttempts - 1, e.getMessage());
                        return attemptSend(msg, remainingAttempts - 1);
                    } else {
                        throw new UnavailableChannelException("Failed to send message after " + config.getMaxRetries() + " attempts", e);
                    }
                });
    }

    @Override
    public void start() throws Exception {
        client.start();
    }

    @Override
    public void shutdown() throws Exception {
        client.shutdown();
    }

    @Override
    public void close() throws Exception {
        shutdown();
    }

    private class DefaultProducerHandler implements ClientProtocolHandler {

        @Override
        public void dispatch(ChannelHandlerContext ctx, TransportMessage msg) {
            if (msg instanceof TransportMessage transportMsg) {
                switch (transportMsg.getType()) {
                    case ACK -> onAck(ctx, transportMsg);
                    case HEARTBEAT -> onHeartbeat(ctx, transportMsg);
                    case DISCONNECT -> onDisconnect(ctx);
                    default -> log.warn("Received unsupported message type: {}", transportMsg.getType());
                }
            }
        }

        @Override
        public void onMessage(ChannelHandlerContext ctx, TransportMessage msg) {
            ctx.channel().writeAndFlush(msg);
            log.debug("Sent message from client: type={}, seq={}", msg.getType(), msg.getSeq());
        }

        @Override
        public void onAck(ChannelHandlerContext ctx, TransportMessage msg) {
            long deliveryTag = msg.getSeq();
            CompletableFuture<Void> future = pendingAcks.remove(deliveryTag);
            if (future != null) {
                // Check if the ACK indicates success
                if (msg.hasAck() && msg.getAck().getSuccess()) {
                    future.complete(null);
                } else {
                    // If ACK indicates failure, complete the future exceptionally to trigger retry
                    String errorMsg = msg.hasAck()
                            ? msg.getAck().getErrorMessage() 
                            : "Unknown error";
                    future.completeExceptionally(new RuntimeException("Broker rejected message: " + errorMsg));
                }
            }
        }

        @Override
        public void onHeartbeat(ChannelHandlerContext ctx, TransportMessage msg) {
            // TODO: Implement heartbeat handling
        }

        @Override
        public void onConnect(ChannelHandlerContext ctx) {
            TransportMessage connectMsg = TransportMessage.newBuilder()
                    .setType(MessageType.CONNECT)
                    .setSeq(0)
                    .setTimestamp(System.currentTimeMillis())
                    .setConnect(com.arth.sakimq.protocol.ConnectPayload.newBuilder()
                            .setClientId(name)
                            .setUsername("")
                            .setPassword("")
                            .build())
                    .build();
            client.send(connectMsg).thenAccept(v -> {
                log.info("CONNECT message sent successfully");
            }).exceptionally(ex -> {
                log.error("Failed to send CONNECT message: {}", ex.getMessage());
                return null;
            });
        }

        @Override
        public void onDisconnect(ChannelHandlerContext ctx) {
            log.info("Connection lost with broker");
            pendingAcks.forEach((k, f) -> f.completeExceptionally(new RuntimeException("Connection lost")));
            pendingAcks.clear();
        }
    }
}