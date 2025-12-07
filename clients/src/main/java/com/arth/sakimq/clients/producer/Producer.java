package com.arth.sakimq.clients.producer;


import com.arth.sakimq.protocol.Message;
import com.arth.sakimq.protocol.TransportMessage;

public interface Producer {

    void send(TransportMessage transportMessage) throws InterruptedException;

    void start() throws InterruptedException;

    void shutdown();
}