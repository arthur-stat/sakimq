package com.arth.sakimq.broker;

import com.arth.sakimq.network.handler.TransportHandler;
import com.arth.sakimq.network.netty.NettyServer;
import com.arth.sakimq.protocol.TransportMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Broker {

    private static final Logger log = LoggerFactory.getLogger(Broker.class);

    private int port;
    private final TopicsManager topicsManager;
    private final NettyServer nettyServer;


    public Broker(int port, TopicsManager topicsManager) {
        this.port = port;
        this.topicsManager = topicsManager;
        this.nettyServer = new NettyServer(port, new BrokerTransportHandler());
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