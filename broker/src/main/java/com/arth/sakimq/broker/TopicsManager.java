package com.arth.sakimq.broker;

import com.arth.sakimq.common.topic.Topic;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TopicsManager {

    private final ConcurrentHashMap<String, Topic> topics;

    public TopicsManager() {
        topics = new ConcurrentHashMap<>();
    }

    public TopicsManager(Map<String, Topic> topics) {
        this.topics = new ConcurrentHashMap<>(topics);
    }
}
