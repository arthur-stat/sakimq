package com.arth.sakimq.common.message;

public interface MessageHandler {

    void onMessage(ReusableMessageEvent event);
}
