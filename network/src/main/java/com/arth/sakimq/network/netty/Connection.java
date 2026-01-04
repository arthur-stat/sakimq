package com.arth.sakimq.network.netty;

import com.arth.sakimq.protocol.MessageType;
import com.arth.sakimq.protocol.TransportMessage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Connection wrapper that holds channel state and sequencing info.
 */
public class Connection {
    private final Channel channel;
    private final String clientId;
    private final long connectTime;
    private volatile long lastHeartbeatTime;
    private volatile boolean isActive;
    private final AtomicLong lastSeq;

    /**
     * Create a new connection wrapper.
     * @param channel netty channel
     * @param clientId client identifier
     */
    public Connection(Channel channel, String clientId) {
        this.channel = channel;
        this.clientId = clientId;
        this.connectTime = System.currentTimeMillis();
        this.lastHeartbeatTime = System.currentTimeMillis();
        this.isActive = true;
        this.lastSeq = new AtomicLong(0);
    }

    /**
     * Update the last heartbeat timestamp.
     */
    public void updateHeartbeat() {
        this.lastHeartbeatTime = System.currentTimeMillis();
    }

    /**
     * Check if heartbeat timed out.
     * @param timeoutMs timeout in millis
     * @return true if timed out
     */
    public boolean isHeartbeatTimeout(long timeoutMs) {
        return System.currentTimeMillis() - lastHeartbeatTime > timeoutMs;
    }

    /**
     * Duration since connection established.
     * @return millis since connect
     */
    public long getConnectionDuration() {
        return System.currentTimeMillis() - connectTime;
    }

    /**
     * Check and update last sequence.
     * @param seq message sequence
     * @return true if this seq is newer
     */
    public boolean checkAndUpdateSeq(long seq) {
        long current;
        do {
            current = lastSeq.get();
            // Duplicate or stale message if seq <= currentSeq (with overflow handling)
            if (isSequenceLessThanOrEqualTo(seq, current)) {
                return false;
            }
            // Try to advance the sequence to the incoming seq
        } while (!lastSeq.compareAndSet(current, seq));
        return true;
    }

    /**
     * Compare sequences per RFC1982 overflow semantics.
     */
    private boolean isSequenceLessThanOrEqualTo(long seq, long current) {
        return Long.compareUnsigned(current - seq, 0x8000000000000000L) < 0;
    }

    /**
     * Send a transport message.
     * @param msg transport message
     */
    public void send(TransportMessage msg) {
        if (channel != null && channel.isActive()) {
            channel.writeAndFlush(msg).addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    // swallow send failure for now
                }
            });
        }
    }

    /**
     * Send a DISCONNECT message.
     */
    public void sendDisconnect() {
        TransportMessage disconnectMsg = TransportMessage.newBuilder()
                .setType(MessageType.DISCONNECT)
                .setSeq(0)
                .setTimestamp(System.currentTimeMillis())
                .build();
        send(disconnectMsg);
    }

    /**
     * Close the connection.
     */
    public void close() {
        isActive = false;
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }

    // Getters
    public Channel getChannel() {
        return channel;
    }

    public String getClientId() {
        return clientId;
    }

    public long getLastHeartbeatTime() {
        return lastHeartbeatTime;
    }

    public boolean isActive() {
        return isActive;
    }

    public long getConnectTime() {
        return connectTime;
    }

    public long getLastSeq() {
        return lastSeq.get();
    }
}
