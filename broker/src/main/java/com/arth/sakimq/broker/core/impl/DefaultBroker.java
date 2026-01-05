package com.arth.sakimq.broker.core.impl;

import com.arth.sakimq.broker.core.Broker;
import com.arth.sakimq.broker.seq.SeqManager;
import com.arth.sakimq.broker.store.FileMessageStore;
import com.arth.sakimq.broker.store.MessageStore;
import com.arth.sakimq.broker.store.OffsetManager;
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
    private final MessageStore messageStore;
    private final OffsetManager offsetManager;
    private final NettyServer server;
    private volatile boolean active = false;
    private final String storeDir = "./data";

    public DefaultBroker() {
        this.name = "Broker-" + UUID.randomUUID();
        this.messageStore = new FileMessageStore(storeDir);
        this.offsetManager = new OffsetManager(storeDir);
        this.topicsManager = new TopicsManager();
        this.sessionManager = new SeqManager();
        this.server = new NettyServer(new DefaultBrokerApplicationProtocolHandler(topicsManager, sessionManager, messageStore, offsetManager));
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
        this.messageStore = new FileMessageStore(storeDir);
        this.offsetManager = new OffsetManager(storeDir);
        this.server = new NettyServer(port, new DefaultBrokerApplicationProtocolHandler(topicsManager, sessionManager, messageStore, offsetManager));
        log.info("Broker initialized with custom port: {}", this.port);
    }

    @Override
    public void start() throws Exception {
        if (!active) {
            synchronized (this) {
                if (!active) {
                    try {
                        // Start stores
                        messageStore.start();
                        offsetManager.load();

                        // Replay messages to restore memory state
                        log.info("Replaying messages from store...");
                        messageStore.replay(0, (offset, msgPack) -> {
                            topicsManager.publish(msgPack);
                        });
                        log.info("Replay completed.");

                        server.start().get();
                        active = true;
                        log.info("Broker {} started successfully.", name);
                    } catch (Exception e) {
                        log.error("Failed to start broker {}: {}", name, e.getMessage());
                        throw new UnavailableChannelException("Failed to start broker", e);
                    }
                }
            }
        } else {
            log.warn("Broker {} is already started.", name);
        }
    }

    @Override
    public void shutdown() throws Exception {
        server.shutdown().get();
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
    public OffsetManager getOffsetManager() {
        return offsetManager;
    }

    @Override
    public MessageStore getMessageStore() {
        return messageStore;
    }

    @Override
    public NettyServer getNettyServer() {
        return server;
    }
}