package com.arth.sakimq.broker.topic;

import com.arth.sakimq.common.constant.LoggerName;
import com.arth.sakimq.protocol.MessagePack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class TopicsManager {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.TOPIC);
    private final ConcurrentHashMap<String, Topic> topics;

    public TopicsManager() {
        topics = new ConcurrentHashMap<>();
    }

    public void register(Topic topic) {
        topics.put(topic.getName(), topic);
    }

    public void publish(MessagePack messagePack) {
        List<String> targetTopics = messagePack.getTopicsList();
        for (String topicName : targetTopics) {
            Topic topic = topics.get(topicName);
            if (topic != null) topic.publish(messagePack.getMessage());
            else log.warn("Topic not found: {}", topicName);
        }
    }

    public void forEach(Consumer<Topic> consumer) {
        topics.values().forEach(consumer);
    }
}