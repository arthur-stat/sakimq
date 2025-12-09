package com.arth.sakimq.clients.config;

import com.arth.sakimq.common.utils.YamlParser;
import com.arth.sakimq.network.config.NettyConfig;

import java.io.IOException;
import java.util.Map;

public class DefaultProducerConfig implements ProducerConfig {

    private static final String DEFAULT_CONFIG_FILE = "config/producer.yml";
    
    // Default values
    private static final int DEFAULT_MAX_RETRIES = 3;

    // Instance variable (no longer static constant)
    private int maxRetries;

    private static volatile ProducerConfig INSTANCE;

    private DefaultProducerConfig() {
        // Load configuration from YAML file
        loadConfig();
    }
    
    private void loadConfig() {
        try {
            Map<String, String> configMap = YamlParser.parseYaml(DEFAULT_CONFIG_FILE);
            
            // Load values from config map, use defaults if not found
            maxRetries = YamlParser.getIntValue(configMap, "maxRetries", DEFAULT_MAX_RETRIES);
        } catch (IOException e) {
            // If config file not found, use default values
            System.out.println("Warning: Could not load config file " + DEFAULT_CONFIG_FILE + ", using default values.");
            maxRetries = DEFAULT_MAX_RETRIES;
        }
    }

    @Override
    public int getMaxRetries() {
        return maxRetries;
    }

    public static ProducerConfig getConfig() {
        if (INSTANCE == null) {
            synchronized (DefaultProducerConfig.class) {
                if (INSTANCE == null) {
                    INSTANCE = new DefaultProducerConfig();
                }
            }
        }
        return INSTANCE;
    }
}
