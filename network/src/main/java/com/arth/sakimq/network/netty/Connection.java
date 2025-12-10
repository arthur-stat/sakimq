package com.arth.sakimq.network.netty;

import com.arth.sakimq.protocol.MessageType;
import com.arth.sakimq.protocol.TransportMessage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 连接对象，封装所有与连接相关的状态信息
 */
public class Connection {
    private final Channel channel;
    private final String clientId;
    private final long connectTime;
    private volatile long lastHeartbeatTime;
    private volatile boolean isActive;
    private final AtomicLong lastSeq;

    /**
     * 创建连接对象
     * @param channel Channel对象
     * @param clientId 客户端ID
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
     * 更新心跳时间
     */
    public void updateHeartbeat() {
        this.lastHeartbeatTime = System.currentTimeMillis();
    }

    /**
     * 检查心跳是否超时
     * @param timeoutMs 超时时间（毫秒）
     * @return 是否超时
     */
    public boolean isHeartbeatTimeout(long timeoutMs) {
        return System.currentTimeMillis() - lastHeartbeatTime > timeoutMs;
    }

    /**
     * 获取连接持续时间
     * @return 连接持续时间（毫秒）
     */
    public long getConnectionDuration() {
        return System.currentTimeMillis() - connectTime;
    }

    /**
     * 检查并更新序列号
     * @param seq 消息序列号
     * @return 是否是新消息
     */
    public boolean checkAndUpdateSeq(long seq) {
        long current;
        do {
            current = lastSeq.get();
            // 重复或旧消息，如果seq <= currentSeq（处理溢出）
            if (isSequenceLessThanOrEqualTo(seq, current)) {
                return false;
            }
            // 尝试将序列号更新为消息的seq
        } while (!lastSeq.compareAndSet(current, seq));
        return true;
    }

    /**
     * 根据RFC1982，在整数溢出的环绕行为下比较序列号
     */
    private boolean isSequenceLessThanOrEqualTo(long seq, long current) {
        return Long.compareUnsigned(current - seq, 0x8000000000000000L) < 0;
    }

    /**
     * 发送消息
     * @param msg 消息
     */
    public void send(TransportMessage msg) {
        if (channel != null && channel.isActive()) {
            channel.writeAndFlush(msg).addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    // 记录发送失败
                }
            });
        }
    }

    /**
     * 发送断开连接消息
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
     * 关闭连接
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