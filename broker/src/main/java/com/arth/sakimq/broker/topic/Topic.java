package com.arth.sakimq.broker.topic;

import com.arth.sakimq.broker.config.QueueConfig;
import com.arth.sakimq.broker.queue.DisruptorQueue;
import com.arth.sakimq.broker.queue.PullQueue;
import com.arth.sakimq.protocol.MessagePack;

import java.util.concurrent.atomic.AtomicInteger;

public class Topic {

    private static final AtomicInteger QUEUE_ID = new AtomicInteger(0);

    private final String name;
    private final String description;
    private final PullQueue queue;

    public Topic(String name) {
        this(name, "");
    }

    public Topic(String name, String description) {
        this.name = name;
        this.description = description;
        this.queue = new DisruptorQueue(QueueConfig.getConfig());
        this.queue.setQueueId(QUEUE_ID.getAndIncrement());
    }

    public void publish(MessagePack messagePack) {
        if (!queue.append(messagePack)) {
            queue.appendBlocking(messagePack);
        }
    }

    public MessagePack poll() {
        return queue.poll();
    }

    public MessagePack getMessage(long sequence) {
        return queue.get(sequence);
    }

    public long getMaxOffset() {
        return queue.getCursor();
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String toString() {
        return "Topic{name='" + name + "', description='" + description + "'}";
    }
}
