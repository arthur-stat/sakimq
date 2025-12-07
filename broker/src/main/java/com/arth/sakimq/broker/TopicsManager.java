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
    
    /**
     * 添加一个新的主题
     * @param topicName 主题名称
     * @param topic 主题对象
     */
    public void addTopic(String topicName, Topic topic) {
        topics.put(topicName, topic);
    }
    
    /**
     * 根据主题名称获取主题
     * @param topicName 主题名称
     * @return 主题对象
     */
    public Topic getTopic(String topicName) {
        return topics.get(topicName);
    }
    
    /**
     * 检查是否存在指定的主题
     * @param topicName 主题名称
     * @return 是否存在
     */
    public boolean containsTopic(String topicName) {
        return topics.containsKey(topicName);
    }
    
    /**
     * 移除主题
     * @param topicName 主题名称
     */
    public void removeTopic(String topicName) {
        topics.remove(topicName);
    }
    
    /**
     * 获取所有主题
     * @return 所有主题的集合
     */
    public ConcurrentHashMap<String, Topic> getAllTopics() {
        return new ConcurrentHashMap<>(topics);
    }
}