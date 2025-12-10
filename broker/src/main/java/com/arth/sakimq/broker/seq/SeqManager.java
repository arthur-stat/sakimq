package com.arth.sakimq.broker.seq;

import com.arth.sakimq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manage the sequence number for each Producer client to ensure message idempotence.
 * Hold by broker.
 */
public class SeqManager {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER);
    private final ConcurrentMap<String, AtomicLong> producerSeqMap = new ConcurrentHashMap<>();

    public SeqManager() {
    }

    /**
     * Checks if a message is new (i.e., its sequence number is greater than the client's last processed sequence number).
     * If it's a new message, updates the sequence number and returns true.
     * If it's a duplicate or old message, returns false.
     *
     * @param clientId Client ID
     * @param seq      Message sequence number
     * @return Whether the message is new
     */
    public boolean checkAndUpdateSeq(String clientId, long seq) {
        // Gets or creates the sequence number counter for this client
        AtomicLong lastSeq = producerSeqMap.computeIfAbsent(clientId, k -> new AtomicLong(0));

        // Loop attempting to update the sequence number
        long current;
        do {
            current = lastSeq.get();
            // Duplicate or old message if seq <= currentSeq (handles overflow)
            if (isSequenceLessThanOrEqualTo(seq, current)) {
                log.debug("Duplicate message received from client {}: seq={} (last processed: {})", clientId, seq, current);
                return false;
            }
            // Try updating sequence number to message's seq
        } while (!lastSeq.compareAndSet(current, seq));

        log.debug("Processed new message from client {}: seq={}", clientId, seq);
        return true;
    }

    /**
     * Referring to RFC1982, compare sequence numbers under the wrap-around behavior of integer overflow.
     */
    private boolean isSequenceLessThanOrEqualTo(long seq, long current) {
        return Long.compareUnsigned(current - seq, 0x8000000000000000L) < 0;
    }

    /**
     * Remove the client's sequence number record
     *
     * @param clientId Client ID
     */
    public void removeClient(String clientId) {
        producerSeqMap.remove(clientId);
        log.info("Removed client seq record: {}", clientId);
    }

    /**
     * Register a new client
     *
     * @param clientId Client ID
     */
    public void registerClient(String clientId) {
        producerSeqMap.computeIfAbsent(clientId, k -> new AtomicLong(-1));
        log.info("Registered new client: {}", clientId);
    }

    /**
     * Get the current sequence number of the client
     *
     * @param clientId Client ID
     * @return Current sequence number, returns -1 if the client does not exist
     */
    public long getCurrentSeq(String clientId) {
        AtomicLong lastSeq = producerSeqMap.get(clientId);
        return lastSeq != null ? lastSeq.get() : -1;
    }
}