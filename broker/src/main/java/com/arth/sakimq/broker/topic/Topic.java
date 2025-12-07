package com.arth.sakimq.broker.topic;

import com.arth.sakimq.broker.queue.PullQueue;
import com.arth.sakimq.protocol.Message;

import java.util.List;

public class Topic {

    private String name;
    private String description;
    private List<PullQueue> queues;

    public void publish(Message message) {
        for (PullQueue queue : queues) {
            queue.append(message);
        }
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
