package com.arth.sakimq.common.queue;

import com.arth.sakimq.common.message.Message;

public interface PullQueue {

    void setQueueId(int queueId);

    boolean append(Message message, int queueId);

    void appendBlocking(Message message, int queueId);

    Message poll();

    Message take();

    boolean isFull();

    void close();
}