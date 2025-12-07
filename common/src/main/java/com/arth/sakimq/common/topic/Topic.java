package com.arth.sakimq.common.topic;

import com.arth.sakimq.common.queue.PullQueue;

import java.util.List;

public class Topic {

    private String name;
    private String description;
    private List<PullQueue> queues;


    public String toString() {
        return "Topic{name='" + name + "', description='" + description + "'}";
    }

    public String getName() {
        return name;
    }
}
