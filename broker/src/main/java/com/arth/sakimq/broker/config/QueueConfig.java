package com.arth.sakimq.broker.config;

import com.arth.sakimq.common.utils.YamlParser;

import java.io.IOException;
import java.util.Map;

public class QueueConfig {

    private static final String DEFAULT_CONFIG_FILE = "config/queue.yml";
    
    // Default values
    private static final int DEFAULT_TIMEOUT = 5;
    private static final int DEFAULT_BUFFER_SIZE = 1024;
    private static final int DEFAULT_CONSUMER_GROUP_SIZE = 1;

    // Instance variables
    private int timeout;
    private int bufferSize;
    private int consumerGroupSize;

    private static volatile QueueConfig INSTANCE;

    private QueueConfig() {
        // Load configuration from YAML file
        loadConfig();
    }
    
    private void loadConfig() {
        try {
            Map<String, String> configMap = YamlParser.parseYaml(DEFAULT_CONFIG_FILE);
            
            // Load values from config map, use defaults if not found
            timeout = YamlParser.getIntValue(configMap, "timeout", DEFAULT_TIMEOUT);
            bufferSize = YamlParser.getIntValue(configMap, "bufferSize", DEFAULT_BUFFER_SIZE);
            consumerGroupSize = YamlParser.getIntValue(configMap, "consumerGroupSize", DEFAULT_CONSUMER_GROUP_SIZE);
        } catch (IOException e) {
            // If config file not found, use default values
            System.out.println("Warning: Could not load config file " + DEFAULT_CONFIG_FILE + ", using default values.");
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
    
    // Getters and setters
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
