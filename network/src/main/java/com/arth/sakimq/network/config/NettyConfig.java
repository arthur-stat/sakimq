package com.arth.sakimq.network.config;

public interface NettyConfig {

    int getPort();
    int getTimeout();
    int getMaxFrameLength();
    int getLengthFieldLength();
    int getInitialBytesToStrip();
}
