package com.arth.sakimq.broker.core;

import com.arth.sakimq.broker.topic.TopicsManager;
import com.arth.sakimq.network.netty.NettyServer;
import com.arth.sakimq.broker.store.MessageStore;
import com.arth.sakimq.broker.store.OffsetManager;

public interface Broker {
    void start() throws Exception;

    void shutdown() throws Exception;

    int getPort();

    TopicsManager getTopicsManager();

    /**
     * Gets the offset manager.
     *
     * @return the offset manager
     */
    OffsetManager getOffsetManager();

    /**
     * Gets the message store.
     *
     * @return the message store
     */
    MessageStore getMessageStore();

    NettyServer getNettyServer();
}