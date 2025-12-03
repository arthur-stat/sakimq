package com.arth.sakimq.broker;

import com.arth.sakimq.common.topic.Topic;

import java.util.concurrent.ConcurrentHashMap;

public class Broker {

    // topic name str -> topic
    private ConcurrentHashMap<String, Topic> topics;
}
