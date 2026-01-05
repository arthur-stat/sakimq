package com.arth.sakimq.broker.store;

import com.arth.sakimq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class OffsetManager {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER);
    private final Path offsetFile;
    private final Map<String, Long> offsets = new ConcurrentHashMap<>();
    private final Properties properties = new Properties();

    public OffsetManager(String storeDir) {
        this.offsetFile = Paths.get(storeDir, "offsets.properties");
    }

    public void load() throws IOException {
        if (Files.exists(offsetFile)) {
            try (FileInputStream fis = new FileInputStream(offsetFile.toFile())) {
                properties.load(fis);
                for (String key : properties.stringPropertyNames()) {
                    try {
                        offsets.put(key, Long.parseLong(properties.getProperty(key)));
                    } catch (NumberFormatException e) {
                        log.warn("Invalid offset for key {}: {}", key, properties.getProperty(key));
                    }
                }
            }
        }
    }

    public void updateOffset(String groupId, String topic, long offset) {
        String key = getGroupTopicKey(groupId, topic);
        offsets.put(key, offset);
        persist(); // Persist immediately for data safety in this simple implementation
    }

    public long getOffset(String groupId, String topic) {
        String key = getGroupTopicKey(groupId, topic);
        return offsets.getOrDefault(key, -1L); // Return -1 if no offset found
    }

    private String getGroupTopicKey(String groupId, String topic) {
        return groupId + ":" + topic;
    }

    public synchronized void persist() {
        try (FileOutputStream fos = new FileOutputStream(offsetFile.toFile())) {
            for (Map.Entry<String, Long> entry : offsets.entrySet()) {
                properties.setProperty(entry.getKey(), String.valueOf(entry.getValue()));
            }
            properties.store(fos, "Consumer Offsets");
        } catch (IOException e) {
            log.error("Failed to persist offsets", e);
        }
    }
}
