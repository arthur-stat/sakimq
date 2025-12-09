package com.arth.sakimq.broker.config;

import com.arth.sakimq.common.constant.LoggerName;
import com.arth.sakimq.common.utils.YamlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class QueueConfig {

    private static final String DEFAULT_CONFIG_FILE = "config/queue.yml";

    private static final int DEFAULT_TIMEOUT = 5;
    private static final int DEFAULT_BUFFER_SIZE = 1024;
    private static final int DEFAULT_CONSUMER_GROUP_SIZE = 1;

    private int timeout;
    private int bufferSize;
    private int consumerGroupSize;

    private static final Logger log = LoggerFactory.getLogger(LoggerName.MESSAGE);
    private static volatile QueueConfig INSTANCE;

    private QueueConfig() {
        try {
            Map<String, String> configMap = YamlParser.parseYaml(DEFAULT_CONFIG_FILE);
            timeout = YamlParser.getIntValue(configMap, "timeout", DEFAULT_TIMEOUT);
            bufferSize = YamlParser.getIntValue(configMap, "bufferSize", DEFAULT_BUFFER_SIZE);
            consumerGroupSize = YamlParser.getIntValue(configMap, "consumerGroupSize", DEFAULT_CONSUMER_GROUP_SIZE);
        } catch (IOException e) {
            log.warn("Failed to load queue config file: " + DEFAULT_CONFIG_FILE + ". Using default values.");
            timeout = DEFAULT_TIMEOUT;
            bufferSize = DEFAULT_BUFFER_SIZE;
            consumerGroupSize = DEFAULT_CONSUMER_GROUP_SIZE;
        }
    }

    public static QueueConfig getConfig() {
        if (INSTANCE == null) {
            synchronized (QueueConfig.class) {
                if (INSTANCE == null) {
                    INSTANCE = new QueueConfig();
                }
            }
        }
        return INSTANCE;
    }

    public int getTimeout() {
        return timeout;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public int getConsumerGroupSize() {
        return consumerGroupSize;
    }
}
