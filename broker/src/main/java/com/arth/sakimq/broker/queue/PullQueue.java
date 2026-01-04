package com.arth.sakimq.broker.queue;


import com.arth.sakimq.protocol.MessagePack;

public interface PullQueue {

    void setQueueId(int queueId);

    boolean append(MessagePack message);

    void appendBlocking(MessagePack message);

    MessagePack poll();

    MessagePack take();

    boolean isFull();

    void close();
}
