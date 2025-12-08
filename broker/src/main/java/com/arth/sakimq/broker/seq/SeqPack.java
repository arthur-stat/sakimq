package com.arth.sakimq.broker.seq;

import io.netty.channel.Channel;

import java.util.concurrent.atomic.AtomicInteger;

public class SeqPack {

    private AtomicInteger seq = new AtomicInteger(0);
    private Channel channel;
}
