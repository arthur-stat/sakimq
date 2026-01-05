package com.arth.sakimq.broker.store;

import com.arth.sakimq.protocol.MessagePack;

import java.io.IOException;
import java.util.function.BiConsumer;

public interface MessageStore {
    void start() throws IOException;
    long append(MessagePack messagePack) throws IOException;
    void replay(long fromOffset, BiConsumer<Long, MessagePack> messageConsumer) throws IOException;
    void shutdown() throws IOException;
}
