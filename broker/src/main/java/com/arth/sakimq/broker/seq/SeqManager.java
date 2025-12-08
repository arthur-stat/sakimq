package com.arth.sakimq.broker.seq;

import com.arth.sakimq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 管理每个Producer客户端的序列号，用于确保消息的幂等性
 */
public class SeqManager {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER);
    private final ConcurrentMap<String, AtomicLong> producerSeqMap = new ConcurrentHashMap<>();

    public SeqManager() {
    }

    /**
     * 检查消息是否是新消息（即序列号大于该客户端的最后处理序列号）
     * 如果是新消息，则更新序列号并返回true
     * 如果是重复消息或旧消息，则返回false
     *
     * @param clientId 客户端ID
     * @param seq      消息序列号
     * @return 是否是新消息
     */
    public boolean checkAndUpdateSeq(String clientId, long seq) {
        // 获取或创建该客户端的序列号计数器
        AtomicLong lastSeq = producerSeqMap.computeIfAbsent(clientId, k -> new AtomicLong(0));

        // 循环尝试更新序列号
        long current;
        do {
            current = lastSeq.get();
            // 如果消息序列号小于等于当前序列号（考虑溢出情况），说明是重复或旧消息
            if (isSequenceLessThanOrEqualTo(seq, current)) {
                log.debug("Duplicate message received from client {}: seq={} (last processed: {})", clientId, seq, current);
                return false;
            }
            // 尝试更新序列号为消息序列号
        } while (!lastSeq.compareAndSet(current, seq));

        log.debug("Processed new message from client {}: seq={}", clientId, seq);
        return true;
    }

    /**
     * RFC 1982 风格的序列号比较
     * 适用于TCP序列号、版本号等
     */
    private boolean isSequenceLessThanOrEqualTo(long seq, long current) {
        return (current - seq) >= 0;
    }

    /**
     * 移除客户端的序列号记录
     *
     * @param clientId 客户端ID
     */
    public void removeClient(String clientId) {
        producerSeqMap.remove(clientId);
        log.info("Removed client seq record: {}", clientId);
    }

    /**
     * 获取客户端的当前序列号
     *
     * @param clientId 客户端ID
     * @return 当前序列号，如果客户端不存在则返回0
     */
    public long getCurrentSeq(String clientId) {
        AtomicLong lastSeq = producerSeqMap.get(clientId);
        return lastSeq != null ? lastSeq.get() : 0;
    }
}