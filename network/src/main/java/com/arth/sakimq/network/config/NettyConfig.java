package com.arth.sakimq.network.config;

import com.arth.sakimq.common.constant.LoggerName;
import com.arth.sakimq.common.utils.YamlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class NettyConfig {

    private static final String DEFAULT_CONFIG_FILE = "config/network.yml";

    private static final int DEFAULT_PORT = 8848;
    private static final int DEFAULT_TIMEOUT = 3;
    private static final int DEFAULT_MAX_FRAME_LENGTH = 64 * 1024 * 1024;
    private static final int DEFAULT_LENGTH_FIELD_LENGTH = 4;
    private static final int DEFAULT_INITIAL_BYTES_TO_STRIP = 4;

    private int port;
    private int timeout;
    private int maxFrameLength;
    private int lengthFieldLength;
    private int initialBytesToStrip;

    private static final Logger log = LoggerFactory.getLogger(LoggerName.MESSAGE);
    private static volatile NettyConfig INSTANCE;

    private NettyConfig() {
        try {
            Map<String, String> configMap = YamlParser.parseYaml(DEFAULT_CONFIG_FILE);
            port = YamlParser.getIntValue(configMap, "port", DEFAULT_PORT);
            timeout = YamlParser.getIntValue(configMap, "timeout", DEFAULT_TIMEOUT);
            maxFrameLength = YamlParser.getIntValue(configMap, "maxFrameLength", DEFAULT_MAX_FRAME_LENGTH);
            lengthFieldLength = YamlParser.getIntValue(configMap, "lengthFieldLength", DEFAULT_LENGTH_FIELD_LENGTH);
            initialBytesToStrip = YamlParser.getIntValue(configMap, "initialBytesToStrip", DEFAULT_INITIAL_BYTES_TO_STRIP);
        } catch (IOException e) {
            log.warn("Failed to load producer config file: " + DEFAULT_CONFIG_FILE + ". Using default values.");
            port = DEFAULT_PORT;
            timeout = DEFAULT_TIMEOUT;
            maxFrameLength = DEFAULT_MAX_FRAME_LENGTH;
            lengthFieldLength = DEFAULT_LENGTH_FIELD_LENGTH;
            initialBytesToStrip = DEFAULT_INITIAL_BYTES_TO_STRIP;
        }
    }

    public static NettyConfig getConfig() {
        if (INSTANCE == null) {
            synchronized (NettyConfig.class) {
                if (INSTANCE == null) {
                    INSTANCE = new NettyConfig();
                }
            }
        }
        return INSTANCE;
    }

    public int getPort() {
        return port;
    }

    public int getTimeout() {
        return timeout;
    }

    public int getMaxFrameLength() {
        return maxFrameLength;
    }

    public int getLengthFieldLength() {
        return lengthFieldLength;
    }

    public int getInitialBytesToStrip() {
        return initialBytesToStrip;
    }
}
