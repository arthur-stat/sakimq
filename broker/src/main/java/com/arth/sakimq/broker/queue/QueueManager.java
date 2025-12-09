package com.arth.sakimq.broker.queue;

import com.arth.sakimq.broker.config.QueueConfig;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class QueueManager {

    private final AtomicInteger idGenerator = new AtomicInteger(0);
    private final ConcurrentHashMap<Integer, DisruptorQueue> queues = new ConcurrentHashMap<>();

    public DisruptorQueue register(DisruptorQueue queue) {
        int id = idGenerator.getAndIncrement();
        PullQueue q = new DisruptorQueue(QueueConfig.getConfig());
        q.setQueueId(id);
        queues.put(id, queue);
        return queue;
    }

    public DisruptorQueue get(int queueId) {
        return queues.get(queueId);
    }

    public void unregister(int queueId) {
        queues.remove(queueId);
    }

    public Collection<DisruptorQueue> allQueues() {
        return queues.values();
    }
}