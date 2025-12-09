package com.arth.sakimq.network.config;

import com.arth.sakimq.common.utils.YamlParser;

import java.io.IOException;
import java.util.Map;

public class DefaultNettyConfig implements NettyConfig {

    private static final String DEFAULT_CONFIG_FILE = "config/network.yml";
    
    // Default values
    private static final int DEFAULT_PORT = 8848;
    private static final int DEFAULT_TIMEOUT = 3;
    private static final int DEFAULT_MAX_FRAME_LENGTH = 64 * 1024 * 1024;
    private static final int DEFAULT_LENGTH_FIELD_LENGTH = 4;
    private static final int DEFAULT_INITIAL_BYTES_TO_STRIP = 4;

    // Instance variables (no longer static constants)
    private int port;
    private int timeout;
    private int maxFrameLength;
    private int lengthFieldLength;
    private int initialBytesToStrip;

    private static volatile NettyConfig INSTANCE;

    private DefaultNettyConfig() {
        // Load configuration from YAML file
        loadConfig();
    }
    
    private void loadConfig() {
        try {
            Map<String, String> configMap = YamlParser.parseYaml(DEFAULT_CONFIG_FILE);
            
            // Load values from config map, use defaults if not found
            port = YamlParser.getIntValue(configMap, "port", DEFAULT_PORT);
            timeout = YamlParser.getIntValue(configMap, "timeout", DEFAULT_TIMEOUT);
            maxFrameLength = YamlParser.getIntValue(configMap, "maxFrameLength", DEFAULT_MAX_FRAME_LENGTH);
            lengthFieldLength = YamlParser.getIntValue(configMap, "lengthFieldLength", DEFAULT_LENGTH_FIELD_LENGTH);
            initialBytesToStrip = YamlParser.getIntValue(configMap, "initialBytesToStrip", DEFAULT_INITIAL_BYTES_TO_STRIP);
        } catch (IOException e) {
            // If config file not found, use default values
            System.out.println("Warning: Could not load config file " + DEFAULT_CONFIG_FILE + ", using default values.");
            port = DEFAULT_PORT;
            timeout = DEFAULT_TIMEOUT;
            maxFrameLength = DEFAULT_MAX_FRAME_LENGTH;
            lengthFieldLength = DEFAULT_LENGTH_FIELD_LENGTH;
            initialBytesToStrip = DEFAULT_INITIAL_BYTES_TO_STRIP;
        }
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public int getTimeout() {
        return timeout;
    }

    @Override
    public int getMaxFrameLength() {
        return maxFrameLength;
    }

    @Override
    public int getLengthFieldLength() {
        return lengthFieldLength;
    }

    @Override
    public int getInitialBytesToStrip() {
        return initialBytesToStrip;
    }

    public static NettyConfig getConfig() {
        if (INSTANCE == null) {
            synchronized (NettyConfig.class) {
                if (INSTANCE == null) {
                    INSTANCE = new DefaultNettyConfig();
                }
            }
        }
        return INSTANCE;
    }
}
