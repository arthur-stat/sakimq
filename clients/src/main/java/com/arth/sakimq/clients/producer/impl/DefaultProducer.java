package com.arth.sakimq.clients.producer.impl;

import com.arth.sakimq.clients.producer.Producer;
import com.arth.sakimq.network.netty.NettyClient;
import com.arth.sakimq.network.config.NettyClientConfig;
import com.arth.sakimq.network.handler.TransportHandler;
import com.arth.sakimq.protocol.Message;
import com.arth.sakimq.protocol.MessagePack;
import com.arth.sakimq.protocol.MessageType;
import com.arth.sakimq.protocol.TransportMessage;
import com.google.protobuf.ByteString;
import io.netty.channel.Channel;
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
import java.util.function.Function;

public class DefaultProducer implements Producer {

    private static final Logger log = LoggerFactory.getLogger(DefaultProducer.class);
    private final String name;
    private final NettyClient client;
    private final AtomicLong seq = new AtomicLong(0);
    private final ConcurrentMap<Long, CompletableFuture<Void>> pendingAcks = new ConcurrentHashMap<>();
    private final DefaultProducerTransportHandler transportHandler;

    private class DefaultProducerTransportHandler implements TransportHandler {
        @Override
        public void onMessage(Channel channel, TransportMessage msg) {
            log.warn("Received unexpected message from broker: {}", msg);
        }

        @Override
        public void onAck(Channel channel, TransportMessage msg) {
            long deliveryTag = msg.getSeq();
            CompletableFuture<Void> future = pendingAcks.remove(deliveryTag);
            if (future != null) {
                future.complete(null);
            }
        }

        @Override
        public void onHeartbeat(Channel channel, TransportMessage msg) {
            // TODO
        }

        @Override
        public void onConnect(Channel channel, TransportMessage msg) {
            // Not used by producer
        }

        @Override
        public void onDisconnect(Channel channel, TransportMessage msg) {
            log.info("Connection lost with broker");
            pendingAcks.forEach((k, f) -> f.completeExceptionally(new RuntimeException("Connection lost")));
            pendingAcks.clear();
        }
    }

    public DefaultProducer(String host, int port) {
        name = "Producer-" + UUID.randomUUID();
        this.transportHandler = new DefaultProducerTransportHandler();
        this.client = new NettyClient(host, port, name, this.transportHandler, new NettyClientConfig());
    }

    public DefaultProducer(String host, int port, String name) {
        this.name = name;
        this.transportHandler = new DefaultProducerTransportHandler();
        this.client = new NettyClient(host, port, name, this.transportHandler, new NettyClientConfig());
    }

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

        attemptSend(transportMessage, NettyClientConfig.maxRetries).join();
    }

    private CompletableFuture<Void> attemptSend(TransportMessage msg, int remainingAttempts) {
        long deliveryTag = msg.getSeq();
        CompletableFuture<Void> ackFuture = new CompletableFuture<>();
        pendingAcks.put(deliveryTag, ackFuture);

        return client.send(msg)
                .thenCompose(v -> ackFuture.orTimeout(NettyClientConfig.timeout, TimeUnit.SECONDS))
                .exceptionallyCompose(ex -> {
                    pendingAcks.remove(deliveryTag);
                    if (remainingAttempts > 1) {
                        log.warn("Send failed, retrying (remaining {}): {}", remainingAttempts - 1, ex.getMessage());
                        return attemptSend(msg, remainingAttempts - 1);
                    } else {
                        throw new RuntimeException("Failed to send message after " + NettyClientConfig.maxRetries + " attempts", ex);
                    }
                });
    }

    public void start() throws Exception {
        client.start();
    }

    public void shutdown() throws Exception {
        client.shutdown();
    }
}