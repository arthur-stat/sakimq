package com.arth.sakimq.clients.config;

import com.arth.sakimq.common.constant.LoggerName;
import com.arth.sakimq.common.utils.YamlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class ConsumerConfig {

    private static final String DEFAULT_CONFIG_FILE = "config/consumer.yml";

    private static final int POLL_GAP = 500;

    private int pollGap;

    private static final Logger log = LoggerFactory.getLogger(LoggerName.CONFIG);
    private static volatile ConsumerConfig INSTANCE;

    private ConsumerConfig() {
        try {
            Map<String, String> configMap = YamlParser.parseYaml(DEFAULT_CONFIG_FILE);
            pollGap = YamlParser.getIntValue(configMap, "pollGap", POLL_GAP);
        } catch (IOException e) {
            log.warn("Failed to load producer config file: " + DEFAULT_CONFIG_FILE + ". Using default values.");
            pollGap = POLL_GAP;
        }
    }

    public static ConsumerConfig getConfig() {
        if (INSTANCE == null) {
            synchronized (ConsumerConfig.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ConsumerConfig();
                }
            }
        }
        return INSTANCE;
    }

    public int getPollGap() {
        return pollGap;
    }
}
