package com.arth.sakimq.common.queue;

import com.arth.sakimq.common.message.Message;
import com.arth.sakimq.common.topic.Topic;
import com.lmax.disruptor.EventFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

/**
 * Reusable message event for Disruptor
 */
public class ReusableMessageEvent implements Serializable {

    public static final EventFactory<ReusableMessageEvent> FACTORY = ReusableMessageEvent::new;
    private int queueId;
    private Message message;

    public void reset(int queueId, Message message) {
        this.queueId = queueId;
        this.message = message;
    }

    public Message getMessage() {
        return message;
    }

    public long getMessageId() {
        return message.getMessageId();
    }

    public String getTopic() {
        return message.getTopic();
    }

    public byte[] getBody() {
        return message.getBody();
    }

    public long getBirthTimestamp() {
        return message.getTimestamp();
    }
}