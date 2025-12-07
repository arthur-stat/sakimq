package com.arth.sakimq.broker.session;

import com.arth.sakimq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.Channel;
import java.util.concurrent.ConcurrentHashMap;

public class SessionManager {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER);
    private final ConcurrentHashMap<String, Channel> sessions;

    public SessionManager() {
        sessions = new ConcurrentHashMap<>();
    }

    public void register(String sessionId, Channel channel) {
        Channel channel1 = sessions.putIfAbsent(sessionId, channel);
        if (channel1 != null) {
            log.error("Session already exists: {}", sessionId);
        }
    }
}
