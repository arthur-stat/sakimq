package com.arth.sakimq.broker.queue;


import com.arth.sakimq.protocol.Message;

public interface PullQueue {

    void setQueueId(int queueId);

    boolean append(Message message);

    void appendBlocking(Message message);

    Message poll();

    Message take();

    boolean isFull();

    void close();
}