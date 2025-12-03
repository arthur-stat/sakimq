package com.arth.sakimq.common.message;

import com.arth.sakimq.common.topic.Topic;

import java.io.Serializable;

public class ReusableMessageEvent implements Serializable {

    public static final EventFactory<ReusableMessageEvent> FACTORY = ReusableMessageEvent::new;
    private long messageId;
    private Topic topic;
    private byte[] body;
    private long birthTimestamp;
    private int retryCount;

    public void reset(long messageId, Topic topic, int queueId, byte[] body, long timestamp) {
        this.messageId = messageId;
        this.topic = topic;
        this.queueId = queueId;
        this.birthTimestamp = timestamp;
        // TODO: 暂仅扩容，不回收
        if (this.body == null || this.body.length < body.length) {
            this.body = new byte[Math.max(body.length * 2, 1024)];
        }
        System.arraycopy(body, 0, this.body, 0, body.length);
    }

    // Getters
    public long getMessageId() {
        return messageId;
    }

    public Topic getTopic() {
        return topic;
    }

    public byte[] getBody() {
        return body;
    }

    public int getBodyLen() {
        return body.length;
    }

    public long getBirthTimestamp() {
        return birthTimestamp;
    }
}