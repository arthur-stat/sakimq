package com.arth.sakimq.common.message;

import com.arth.sakimq.common.topic.Topic;
import com.lmax.disruptor.EventFactory;

import java.io.Serializable;

/**
 * Reusable message event for Disruptor
 */
public class ReusableMessageEvent implements Serializable {

    public static final EventFactory<ReusableMessageEvent> FACTORY = ReusableMessageEvent::new;
    private long messageId;
    private int queueId;
    private Topic topic;
    private byte[] body;
    private long bodyLen;
    private long birthTimestamp;

    public void reset(long messageId, Topic topic, int queueId, byte[] body, long timestamp) {
        this.messageId = messageId;
        this.topic = topic;
        this.queueId = queueId;
        this.birthTimestamp = timestamp;
        // TODO: just expand buffer, never gc
        if (this.body == null || this.body.length < body.length) {
            this.body = new byte[Math.max(body.length * 2, 1024)];
        }
        System.arraycopy(body, 0, this.body, 0, body.length);
        this.bodyLen = body.length;
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

    public int getBodyLen() {
        return body.length;
    }

    public long getBirthTimestamp() {
        return birthTimestamp;
    }
}