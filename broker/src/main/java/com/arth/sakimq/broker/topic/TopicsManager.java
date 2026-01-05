package com.arth.sakimq.broker.topic;

import com.arth.sakimq.common.constant.LoggerName;
import com.arth.sakimq.protocol.MessagePack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

public class TopicsManager {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.TOPIC);
    private final ConcurrentMap<String, Topic> topics;

    public TopicsManager() {
        topics = new ConcurrentHashMap<>();
    }

    public void register(Topic topic) {
        topics.put(topic.getName(), topic);
    }

    public void publish(MessagePack messagePack) {
        List<String> targetTopics = messagePack.getTopicsList();
        for (String topicName : targetTopics) {
            Topic topic = topics.computeIfAbsent(topicName, Topic::new);
            topic.publish(messagePack);
        }
    }

    /**
     * Polls the first available message pack from the requested topics.
     * If topics list is empty, any available topic will be used.
     */
    public MessagePack poll(List<String> targetTopics) {
        if (targetTopics == null || targetTopics.isEmpty()) {
            for (Topic topic : topics.values()) {
                MessagePack pack = topic.poll();
                if (pack != null) return pack;
            }
            return null;
        }

        for (String topicName : targetTopics) {
            Topic topic = topics.get(topicName);
            if (topic == null) {
                continue;
            }
            MessagePack pack = topic.poll();
            if (pack != null) {
                return pack;
            }
        }
        return null;
    }

    public void forEach(Consumer<Topic> consumer) {
        topics.values().forEach(consumer);
    }

    public Topic getTopic(String name) {
        return topics.get(name);
    }
}
