package com.arth.sakimq.common.message;

public interface MessageQueue {

    boolean append(Message message);

    void appendBlocking(Message message);

    boolean isFull();

    void close();
}
