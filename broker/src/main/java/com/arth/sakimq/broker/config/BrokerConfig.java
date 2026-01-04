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
    private static final int DEFAULT_ACK_MAX_RETRIES = 2;  // equals 3 attempts total
    private static final long DEFAULT_ACK_RETRY_DELAY_MS = 100;
    private static final boolean DEFAULT_MESSAGE_LOG_ENABLED = false;
    private static final long DEFAULT_MESSAGE_LOG_MAX_BYTES = 10 * 1024 * 1024;
    private static final String DEFAULT_MESSAGE_LOG_DIR = "logs";
    private static final boolean DEFAULT_MESSAGE_LOG_INCLUDE_BODY = false;

    private long heartbeatTimeoutMs;
    private int ackMaxRetries;
    private long ackRetryDelayMs;
    private boolean messageLogEnabled;
    private long messageLogMaxBytes;
    private String messageLogDir;
    private boolean messageLogIncludeBody;

    private static final Logger log = LoggerFactory.getLogger(LoggerName.CONFIG);
    private static volatile BrokerConfig INSTANCE;

    private BrokerConfig() {
        try {
            Map<String, String> configMap = YamlParser.parseYaml(DEFAULT_CONFIG_FILE);
            heartbeatTimeoutMs = YamlParser.getLongValue(configMap, "heartbeatTimeoutMs", DEFAULT_HEARTBEAT_TIMEOUT_MS);
            ackMaxRetries = YamlParser.getIntValue(configMap, "ackMaxRetries", DEFAULT_ACK_MAX_RETRIES);
            ackRetryDelayMs = YamlParser.getLongValue(configMap, "ackRetryDelayMs", DEFAULT_ACK_RETRY_DELAY_MS);
            messageLogEnabled = YamlParser.getBooleanValue(configMap, "messageLogEnabled", DEFAULT_MESSAGE_LOG_ENABLED);
            messageLogMaxBytes = YamlParser.getLongValue(configMap, "messageLogMaxBytes", DEFAULT_MESSAGE_LOG_MAX_BYTES);
            messageLogDir = YamlParser.getStringValue(configMap, "messageLogDir", DEFAULT_MESSAGE_LOG_DIR);
            messageLogIncludeBody = YamlParser.getBooleanValue(configMap, "messageLogIncludeBody", DEFAULT_MESSAGE_LOG_INCLUDE_BODY);
        } catch (IOException e) {
            log.warn("Failed to load broker config file: " + DEFAULT_CONFIG_FILE + ". Using default values.");
            heartbeatTimeoutMs = DEFAULT_HEARTBEAT_TIMEOUT_MS;
            ackMaxRetries = DEFAULT_ACK_MAX_RETRIES;
            ackRetryDelayMs = DEFAULT_ACK_RETRY_DELAY_MS;
            messageLogEnabled = DEFAULT_MESSAGE_LOG_ENABLED;
            messageLogMaxBytes = DEFAULT_MESSAGE_LOG_MAX_BYTES;
            messageLogDir = DEFAULT_MESSAGE_LOG_DIR;
            messageLogIncludeBody = DEFAULT_MESSAGE_LOG_INCLUDE_BODY;
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

    public boolean isMessageLogEnabled() {
        return messageLogEnabled;
    }

    public long getMessageLogMaxBytes() {
        return messageLogMaxBytes;
    }

    public String getMessageLogDir() {
        return messageLogDir;
    }

    public boolean isMessageLogIncludeBody() {
        return messageLogIncludeBody;
    }
}
