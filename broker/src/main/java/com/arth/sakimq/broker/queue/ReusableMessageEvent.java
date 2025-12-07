package com.arth.sakimq.broker.queue;

import com.arth.sakimq.protocol.Message;
import com.google.protobuf.ByteString;
import com.lmax.disruptor.EventFactory;

import java.io.Serializable;

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

    public ByteString getBody() {
        return message.getBody();
    }

    public long getBirthTimestamp() {
        return message.getTimestamp();
    }
}