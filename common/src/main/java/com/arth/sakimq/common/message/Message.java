package com.arth.sakimq.common.message;

import com.arth.sakimq.common.topic.Topic;

import java.util.Map;

public class Message {

    private long messageId;
    private Topic topic;
    private Map<String, Object> headers;
    private byte[] body;
    private long birthTimestamp;
    private int retryCount;

    public Message(Topic topic, Map<String, Object> headers, byte[] body) {
        this.messageId = System.nanoTime();
        this.topic = topic;
        this.headers = headers;
        this.body = body;
        this.birthTimestamp = System.currentTimeMillis();
    }

    public long getMessageId() {
        return messageId;
    }

    public Topic getTopic() {
        return topic;
    }

    public byte[] getBody() {
        return body;
    }

    public long getTimestamp() {
        return birthTimestamp;
    }

    public int getRetryCount() {
        return retryCount;
    }
}
