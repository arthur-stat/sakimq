package com.arth.sakimq.broker.config;

import com.arth.sakimq.common.constant.LoggerName;
import com.arth.sakimq.common.utils.YamlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Broker-level configuration loaded from YAML.
 */
public class BrokerConfig {

    private static final String DEFAULT_CONFIG_FILE = "config/broker.yml";

    private static final long DEFAULT_HEARTBEAT_TIMEOUT_MS = 60000;
    private static final int DEFAULT_ACK_MAX_RETRIES = 2; // equals 3 attempts total
    private static final long DEFAULT_ACK_RETRY_DELAY_MS = 100;

    private long heartbeatTimeoutMs;
    private int ackMaxRetries;
    private long ackRetryDelayMs;

    private static final Logger log = LoggerFactory.getLogger(LoggerName.CONFIG);
    private static volatile BrokerConfig INSTANCE;

    private BrokerConfig() {
        try {
            Map<String, String> configMap = YamlParser.parseYaml(DEFAULT_CONFIG_FILE);
            heartbeatTimeoutMs = YamlParser.getLongValue(configMap, "heartbeatTimeoutMs", DEFAULT_HEARTBEAT_TIMEOUT_MS);
            ackMaxRetries = YamlParser.getIntValue(configMap, "ackMaxRetries", DEFAULT_ACK_MAX_RETRIES);
            ackRetryDelayMs = YamlParser.getLongValue(configMap, "ackRetryDelayMs", DEFAULT_ACK_RETRY_DELAY_MS);
        } catch (IOException e) {
            log.warn("Failed to load broker config file: " + DEFAULT_CONFIG_FILE + ". Using default values.");
            heartbeatTimeoutMs = DEFAULT_HEARTBEAT_TIMEOUT_MS;
            ackMaxRetries = DEFAULT_ACK_MAX_RETRIES;
            ackRetryDelayMs = DEFAULT_ACK_RETRY_DELAY_MS;
        }
    }

    public static BrokerConfig getConfig() {
        if (INSTANCE == null) {
            synchronized (BrokerConfig.class) {
                if (INSTANCE == null) {
                    INSTANCE = new BrokerConfig();
                }
            }
        }
        return INSTANCE;
    }

    public long getHeartbeatTimeoutMs() {
        return heartbeatTimeoutMs;
    }

    public int getAckMaxRetries() {
        return ackMaxRetries;
    }

    public long getAckRetryDelayMs() {
        return ackRetryDelayMs;
    }
}
