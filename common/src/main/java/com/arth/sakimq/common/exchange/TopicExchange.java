package com.arth.sakimq.common.exchange;

import com.arth.sakimq.common.message.Message;
import com.arth.sakimq.common.topic.Topic;

import java.util.Set;

public class TopicExchange {

    private Set<Topic> topics;

    public void publish(String tag, Message message) {
        for (Topic topic : topics) {

        }
    }
}
