package com.arth.sakimq.broker.core.impl;

import com.arth.sakimq.broker.core.Broker;
import com.arth.sakimq.broker.seq.SeqManager;
import com.arth.sakimq.broker.topic.TopicsManager;
import com.arth.sakimq.common.exception.UnavailableChannelException;
import com.arth.sakimq.network.netty.NettyServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class DefaultBroker implements Broker {

    private static final Logger log = LoggerFactory.getLogger(DefaultBroker.class);

    private final int port;
    private final String name;
    private final TopicsManager topicsManager;
    private final SeqManager sessionManager;
    private final NettyServer server;
    private volatile boolean active = false;

    public DefaultBroker() {
        this.name = "Broker-" + UUID.randomUUID();
        this.server = new NettyServer(new DefaultBrokerApplicationProtocolHandler(topicsManager = new TopicsManager(), sessionManager = new SeqManager()));
        this.port = server.getPort();
        log.info("Broker initialized with custom port: {}", this.port);
    }

    public DefaultBroker(int port) {
        this(port, "Broker-" + UUID.randomUUID());
    }

    public DefaultBroker(int port, String name) {
        this.port = port;
        this.name = name;
        this.topicsManager = new TopicsManager();
        this.sessionManager = new SeqManager();
        this.server = new NettyServer(port, new DefaultBrokerApplicationProtocolHandler(topicsManager, sessionManager));
        log.info("Broker initialized with custom port: {}", this.port);
    }

    @Override
    public CompletableFuture<Void> start() {
        if (!active) {
            synchronized (this) {
                if (!active) {
                    try {
                        CompletableFuture<Void> future = server.start();
                        active = true;
                        log.info("Broker {} started successfully.", name);
                        return future;
                    } catch (Exception e) {
                        log.error("Failed to start broker {}: {}", name, e.getMessage());
                        throw new UnavailableChannelException("Failed to start broker", e);
                    }
                } else {
                    return null;
                }
            }
        } else {
            log.warn("Consumer {} is already started.", name);
            return null;
        }
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        return server.shutdown();
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public TopicsManager getTopicsManager() {
        return topicsManager;
    }

    @Override
    public NettyServer getNettyServer() {
        return server;
    }
}