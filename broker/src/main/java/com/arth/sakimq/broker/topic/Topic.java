package com.arth.sakimq.broker.topic;

import com.arth.sakimq.broker.config.QueueConfig;
import com.arth.sakimq.broker.queue.DisruptorQueue;
import com.arth.sakimq.broker.queue.PullQueue;
import com.arth.sakimq.protocol.MessagePack;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Topic {

    private static final AtomicInteger QUEUE_ID = new AtomicInteger(0);

    private final String name;
    private final String description;
    private final List<PullQueue> queues = new ArrayList<>();

    public Topic(String name) {
        this(name, "");
    }

    public Topic(String name, String description) {
        this.name = name;
        this.description = description;
        int consumerGroupSize = QueueConfig.getConfig().getConsumerGroupSize();
        for (int i = 0; i < consumerGroupSize; i++) {
            PullQueue queue = new DisruptorQueue(QueueConfig.getConfig());
            queue.setQueueId(QUEUE_ID.getAndIncrement());
            queues.add(queue);
        }
    }

    public void publish(MessagePack messagePack) {
        for (PullQueue queue : queues) {
            if (!queue.append(messagePack)) {
                queue.appendBlocking(messagePack);
            }
        }
    }

    public MessagePack poll() {
        for (PullQueue queue : queues) {
            MessagePack pack = queue.poll();
            if (pack != null) {
                return pack;
            }
        }
        return null;
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
