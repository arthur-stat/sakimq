package com.arth.sakimq.broker.core.impl;

import com.arth.sakimq.broker.core.Broker;
import com.arth.sakimq.broker.seq.SeqManager;
import com.arth.sakimq.broker.topic.TopicsManager;
import com.arth.sakimq.network.config.DefaultNettyConfig;
import com.arth.sakimq.network.netty.NettyServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class DefaultBroker implements Broker {

    private static final Logger log = LoggerFactory.getLogger(DefaultBroker.class);

    private final int port;
    private final TopicsManager topicsManager;
    private final SeqManager sessionManager;
    private final NettyServer nettyServer;

    // Default constructor that uses the default config
    public DefaultBroker() {
        // Initialize with the default NettyConfig (which loads from config/network.yml)
        this.nettyServer = new NettyServer(new DefaultBrokerApplicationProtocolHandler(topicsManager = new TopicsManager(), sessionManager = new SeqManager()));
        this.port = nettyServer.getPort(); // Get port from the config
        log.info("DefaultBroker initialized with port: {}", this.port);
    }
    
    // Constructor that accepts custom port for backward compatibility
    public DefaultBroker(int port) {
        // This constructor is for backward compatibility
        this.port = port;
        this.topicsManager = new TopicsManager();
        this.sessionManager = new SeqManager();
        this.nettyServer = new NettyServer(port, new DefaultBrokerApplicationProtocolHandler(topicsManager, sessionManager));
        log.info("DefaultBroker initialized with custom port: {}", this.port);
    }

    public CompletableFuture<Void> start() throws InterruptedException {
        return nettyServer.start();
    }

    public CompletableFuture<Void> shutdown() {
        return nettyServer.shutdown();
    }

    public int getPort() {
        return port;
    }

    public TopicsManager getTopicsManager() {
        return topicsManager;
    }

    public NettyServer getNettyServer() {
        return nettyServer;
    }
}