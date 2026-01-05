package com.arth.sakimq.clients.consumer.impl;

import com.arth.sakimq.clients.config.ConsumerConfig;
import com.arth.sakimq.clients.consumer.ConsumerGroup;
import com.arth.sakimq.common.constant.LoggerName;
import com.arth.sakimq.common.exception.UnavailableChannelException;
import com.arth.sakimq.network.config.NettyConfig;
import com.arth.sakimq.network.handler.ClientProtocolHandler;
import com.arth.sakimq.network.netty.NettyClient;
import com.arth.sakimq.protocol.*;
import com.arth.sakimq.protocol.ConnectPayload;
import com.arth.sakimq.protocol.MessagePack;
import com.arth.sakimq.protocol.MessageType;
import com.arth.sakimq.protocol.OffsetCommitPayload;
import com.arth.sakimq.protocol.PollRequest;
import com.arth.sakimq.protocol.PollResponse;
import com.arth.sakimq.protocol.TransportMessage;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class SingleConsumer implements ConsumerGroup, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.CONSUMER);
    private final String name;
    private final NettyClient client;
    private final ConsumerConfig config;
    private final AtomicLong pollSeq = new AtomicLong(0);
    private final ConcurrentMap<Long, CompletableFuture<PollResponse>> pendingPolls = new ConcurrentHashMap<>();
    private final BlockingQueue<MessagePack> inbox = new LinkedBlockingQueue<>();
    private final List<String> subscribedTopics = new CopyOnWriteArrayList<>();
    private volatile Consumer<MessagePack> messageListener;
    private volatile boolean active = false;
    private Thread pollerThread;
    private final CountDownLatch connectLatch = new CountDownLatch(1);

    public SingleConsumer() {
        this("Consumer-" + UUID.randomUUID());
    }

    public SingleConsumer(String name) {
        this(name, ConsumerConfig.getConfig(), NettyConfig.getConfig());
    }

    public SingleConsumer(String name, ConsumerConfig consumerConfig, NettyConfig nettyConfig) {
        this.name = name;
        this.client = new NettyClient(new SingleConsumer.SingleConsumerHandler(), nettyConfig);
        this.client.setSendStrategy(NettyClient.SendStrategy.RANDOM);
        this.config = consumerConfig;
    }

    @Override
    public ConsumerGroup subscribe(List<String> topics) {
        subscribedTopics.clear();
        subscribedTopics.addAll(topics);
        return this;
    }

    @Override
    public ConsumerGroup onMessage(Consumer<MessagePack> listener) {
        this.messageListener = listener;
        return this;
    }

    @Override
    public MessagePack pollLocal() {
        return inbox.poll();
    }

    @Override
    public ConsumerGroup addBroker(String host, int port) {
        client.addConnection(host, port);
        return this;
    }

    @Override
    public ConsumerGroup removeBroker(String host, int port) {
        client.removeConnection(host, port);
        return this;
    }

    @Override
    public void start() {
        if (!active) {
            synchronized (this) {
                if (!active) {
                    try {
                        client.start();
                        active = true;
                        startPolling();
                        log.info("Consumer {} started successfully.", name);
                    } catch (Exception e) {
                        log.error("Failed to start consumer {}: {}", name, e.getMessage());
                        throw new UnavailableChannelException("Failed to start consumer", e);
                    }
                }
            }
        } else {
            log.warn("Consumer {} is already started.", name);
        }
    }

    @Override
    public void shutdown() {
        active = false;
        if (pollerThread != null) {
            pollerThread.interrupt();
        }
        pendingPolls.forEach((k, f) -> f.complete(null));
        pendingPolls.clear();
        try {
            client.shutdown();
        } catch (Exception e) {
            throw new UnavailableChannelException("Failed to shutdown consumer", e);
        }
    }

    @Override
    public void close() throws Exception {
        shutdown();
    }

    private void startPolling() {
        pollerThread = Thread.ofVirtual().start(this::pollLoop);
    }

    private void pollLoop() {
        try {
            if (!connectLatch.await(10, TimeUnit.SECONDS)) {
                log.error("Timeout waiting for consumer {} to connect.", name);
                return;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        while (active) {
            try {
                PollResponse response = pollOnce().join();
                if (response != null && response.hasMessagePack()) {
                    deliver(response.getMessagePack());
                    commitOffset(response.getTopic(), response.getOffset() + 1);
                }
            } catch (Exception e) {
                log.warn("Polling failed for {}: {}", name, e.getMessage());
            }

            try {
                Thread.sleep(config.getPollGap());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void commitOffset(String topic, long offset) {
        TransportMessage commitMsg = TransportMessage.newBuilder()
                .setType(MessageType.OFFSET_COMMIT)
                .setSeq(pollSeq.incrementAndGet())
                .setTimestamp(System.currentTimeMillis())
                .setOffsetCommit(OffsetCommitPayload.newBuilder()
                        .setGroupId(name)
                        .setTopic(topic)
                        .setOffset(offset)
                        .build())
                .build();
        client.send(commitMsg);
    }

    private CompletableFuture<PollResponse> pollOnce() {
        long seq = pollSeq.incrementAndGet();
        CompletableFuture<PollResponse> responseFuture = new CompletableFuture<>();
        pendingPolls.put(seq, responseFuture);

        TransportMessage request = TransportMessage.newBuilder()
                .setType(MessageType.POLL_REQUEST)
                .setSeq(seq)
                .setTimestamp(System.currentTimeMillis())
                .setPollRequest(PollRequest.newBuilder().addAllTopics(subscribedTopics).build())
                .build();

        return client.send(request)
                .thenCompose(v -> responseFuture)
                .orTimeout(config.getPollGap(), TimeUnit.MILLISECONDS)
                .whenComplete((res, ex) -> pendingPolls.remove(seq))
                .exceptionally(ex -> null);
    }

    private void deliver(MessagePack messagePack) {
        inbox.offer(messagePack);
        Consumer<MessagePack> listener = this.messageListener;
        if (listener != null) {
            try {
                listener.accept(messagePack);
            } catch (Exception e) {
                log.warn("Message listener threw exception: {}", e.getMessage());
            }
        }
    }

    private class SingleConsumerHandler implements ClientProtocolHandler {

        private volatile long connectSeq = -1;

        @Override
        public void dispatch(ChannelHandlerContext ctx, TransportMessage msg) {
            switch (msg.getType()) {
                case POLL_RESPONSE -> onPollResponse(ctx, msg);
                case HEARTBEAT -> onHeartbeat(ctx, msg);
                case ACK -> onAck(ctx, msg);
                case DISCONNECT -> onDisconnect(ctx);
                default -> log.warn("Received unsupported message type: {}", msg.getType());
            }
        }

        @Override
        public void onMessage(ChannelHandlerContext ctx, TransportMessage msg) {
            // Consumer doesn't initiate MESSAGE sends
        }

        @Override
        public void onAck(ChannelHandlerContext ctx, TransportMessage msg) {
            log.debug("Received ACK for consumer {}: seq={}", name, msg.getSeq());
            if (msg.getSeq() == connectSeq && msg.hasAck() && msg.getAck().getSuccess()) {
                connectLatch.countDown();
                log.info("Consumer {} registered successfully.", name);
            }
        }

        @Override
        public void onHeartbeat(ChannelHandlerContext ctx, TransportMessage msg) {
            log.debug("Received heartbeat from broker for consumer {}", name);
        }

        @Override
        public void onConnect(ChannelHandlerContext ctx) {
            connectSeq = pollSeq.incrementAndGet();
            TransportMessage connectMsg = TransportMessage.newBuilder()
                    .setType(MessageType.CONNECT)
                    .setSeq(connectSeq)
                    .setTimestamp(System.currentTimeMillis())
                    .setConnect(ConnectPayload.newBuilder()
                            .setClientId(name)
                            .setUsername("")
                            .setPassword("")
                            .build())
                    .build();
            client.send(connectMsg).thenAccept(v -> log.info("CONNECT message sent from consumer {}", name))
                    .exceptionally(ex -> {
                        log.error("Failed to send CONNECT for consumer {}: {}", name, ex.getMessage());
                        return null;
                    });
        }

        @Override
        public void onDisconnect(ChannelHandlerContext ctx) {
            active = false;
            pendingPolls.forEach((k, f) -> f.completeExceptionally(new RuntimeException("Disconnected from broker")));
            pendingPolls.clear();
            log.info("Connection lost with broker for consumer {}", name);
        }

        private void onPollResponse(ChannelHandlerContext ctx, TransportMessage msg) {
            PollResponse response = null;
            if (msg.hasPollResponse()) {
                response = msg.getPollResponse();
            }
            
            CompletableFuture<PollResponse> future = pendingPolls.remove(msg.getSeq());
            if (future != null) {
                future.complete(response);
            }
        }
    }
}