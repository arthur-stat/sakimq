package com.arth.sakimq.common.topic;

import com.arth.sakimq.common.message.Message;
import com.arth.sakimq.common.message.MessageQueue;

import java.util.List;

public class Topic {

    private String name;
    private String description;
    private List<MessageQueue> queues;

    public void publish(Message message) {
        for (MessageQueue queue : queues) {
            queue.append(message);
        }
    }

    public String toString() {
        return "Topic{name='" + name + "', description='" + description + "'}";
    }
}
