package com.arth.sakimq.common.message;

import com.arth.sakimq.common.topic.Topic;

public class Message {

    private long messageId;
    private Topic topic;
    private byte[] body;
    private long birthTimestamp;
    private int retryCount;

    public Message(Topic topic, byte[] body) {
        this.messageId = System.nanoTime();
        this.topic = topic;
        this.body = body;
        this.timestamp = System.currentTimeMillis();
    }

}
