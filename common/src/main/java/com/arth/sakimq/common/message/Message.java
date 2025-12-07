package com.arth.sakimq.common.message;

import com.arth.sakimq.common.topic.Topic;

import java.util.Map;

/**
 * Logic immutable message
 */
public class Message {

    private int messageId;
    private String topic;
    private Map<String, Object> headers;
    private byte[] body;
    private long birthTimestamp;

    public Message(int messageId, String topic, Map<String, Object> headers, byte[] body, long timestamp) {
        this.messageId = messageId;
        this.topic = topic;
        this.headers = headers;
        this.body = body;
        this.birthTimestamp = timestamp;
    }

    public Message(String topic, Map<String, Object> headers, byte[] body) {
        this.messageId = Math.toIntExact(System.nanoTime());
        this.topic = topic;
        this.headers = headers;
        this.body = body;
        this.birthTimestamp = System.currentTimeMillis();
    }

    public int getMessageId() {
        return messageId;
    }

    public String getTopic() {
        return topic;
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public byte[] getBody() {
        return body;
    }

    public long getTimestamp() {
        return birthTimestamp;
    }
}
