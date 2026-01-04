package com.arth.sakimq.broker.core;

import com.arth.sakimq.broker.topic.TopicsManager;
import com.arth.sakimq.network.netty.NettyServer;

import java.util.concurrent.CompletableFuture;

public interface Broker {

    CompletableFuture<Void> start();

    CompletableFuture<Void> shutdown();

    int getPort();

    TopicsManager getTopicsManager();

    NettyServer getNettyServer();
}
