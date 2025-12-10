package com.arth.sakimq.clients.config;

import com.arth.sakimq.common.constant.LoggerName;
import com.arth.sakimq.common.utils.YamlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class ProducerConfig {

    private static final String DEFAULT_CONFIG_FILE = "config/producer.yml";

    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final int DEFAULT_TIMEOUT = 3000;

    private int maxRetries;
    private int timeout;

    private static final Logger log = LoggerFactory.getLogger(LoggerName.CONFIG);
    private static volatile ProducerConfig INSTANCE;

    private ProducerConfig() {
        try {
            Map<String, String> configMap = YamlParser.parseYaml(DEFAULT_CONFIG_FILE);
            maxRetries = YamlParser.getIntValue(configMap, "maxRetries", DEFAULT_MAX_RETRIES);
            timeout = YamlParser.getIntValue(configMap, "timeout", DEFAULT_TIMEOUT);
        } catch (IOException e) {
            log.warn("Failed to load producer config file: " + DEFAULT_CONFIG_FILE + ". Using default values.");
            maxRetries = DEFAULT_MAX_RETRIES;
            timeout = DEFAULT_TIMEOUT;
        }
    }

    public static ProducerConfig getConfig() {
        if (INSTANCE == null) {
            synchronized (ProducerConfig.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ProducerConfig();
                }
            }
        }
        return INSTANCE;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public int getTimeout() {
        return timeout;
    }
}
