package com.arth.sakimq.clients.consumer.impl;

import com.arth.sakimq.clients.config.ConsumerConfig;
import com.arth.sakimq.clients.consumer.ConsumerGroup;
import com.arth.sakimq.common.exception.UnavailableChannelException;
import com.arth.sakimq.network.config.NettyConfig;
import com.arth.sakimq.network.handler.ClientProtocolHandler;
import com.arth.sakimq.network.netty.NettyClient;
import com.arth.sakimq.protocol.TransportMessage;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SingleConsumer implements ConsumerGroup, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SingleConsumer.class);
    private final String name;
    private final NettyClient client;
    private final ConsumerConfig config;
    private volatile boolean active = false;

    public SingleConsumer() {
        this("Consumer-" + UUID.randomUUID());
    }

    public SingleConsumer(String name) {
        this(name, ConsumerConfig.getConfig(), NettyConfig.getConfig());
    }

    public SingleConsumer(String name, ConsumerConfig consumerConfig, NettyConfig nettyConfig) {
        this.name = name;
        this.client = new NettyClient(new SingleConsumer.SingleConsumerHandler(), nettyConfig);
        this.config = consumerConfig;
    }

    @Override
    public ConsumerGroup addBroker(String host, int port) {
        client.addConnection(host, port);
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

    }

    @Override
    public void close() throws Exception {
    }

    private class SingleConsumerHandler implements ClientProtocolHandler {

        @Override
        public void dispatch(ChannelHandlerContext ctx, TransportMessage msg) {

        }

        @Override
        public void onMessage(ChannelHandlerContext ctx, TransportMessage msg) {

        }

        @Override
        public void onAck(ChannelHandlerContext ctx, TransportMessage msg) {

        }

        @Override
        public void onHeartbeat(ChannelHandlerContext ctx, TransportMessage msg) {

        }

        @Override
        public void onConnect(ChannelHandlerContext ctx) {

        }

        @Override
        public void onDisconnect(ChannelHandlerContext ctx) {

        }
    }
}
