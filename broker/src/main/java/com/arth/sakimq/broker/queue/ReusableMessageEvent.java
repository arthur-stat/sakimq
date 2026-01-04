package com.arth.sakimq.broker.queue;

import com.arth.sakimq.protocol.MessagePack;
import com.lmax.disruptor.EventFactory;

import java.io.Serializable;

/**
 * Reusable message event for Disruptor
 */
public class ReusableMessageEvent implements Serializable {

    public static final EventFactory<ReusableMessageEvent> FACTORY = ReusableMessageEvent::new;
    private int queueId;
    private MessagePack messagePack;

    public void reset(int queueId, MessagePack messagePack) {
        this.queueId = queueId;
        this.messagePack = messagePack;
    }

    public MessagePack getMessagePack() {
        return messagePack;
    }
}
