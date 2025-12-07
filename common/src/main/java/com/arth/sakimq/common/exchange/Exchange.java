package com.arth.sakimq.common.exchange;

import com.arth.sakimq.common.message.Message;

public abstract class Exchange {

    public abstract void publish(String tag, Message message);
}
