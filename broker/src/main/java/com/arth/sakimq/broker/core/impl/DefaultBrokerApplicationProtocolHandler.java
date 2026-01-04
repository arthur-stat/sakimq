package com.arth.sakimq.broker.core.impl;

import com.arth.sakimq.broker.config.BrokerConfig;
import com.arth.sakimq.broker.seq.SeqManager;
import com.arth.sakimq.broker.topic.TopicsManager;
import com.arth.sakimq.common.constant.LoggerName;
import com.arth.sakimq.network.handler.BrokerProtocolHandler;
import com.arth.sakimq.network.netty.Connection;
import com.arth.sakimq.network.netty.ConnectionManager;
import com.arth.sakimq.protocol.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class DefaultBrokerApplicationProtocolHandler extends ChannelInboundHandlerAdapter implements BrokerProtocolHandler {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER);
    private final TopicsManager topicsManager;
    private final SeqManager seqManager;
    private final BrokerConfig brokerConfig = BrokerConfig.getConfig();
    // ACK响应统计
    private final ConcurrentMap<String, AckStats> ackStatsMap = new ConcurrentHashMap<>();

    // 心跳超时检测定时器
    private final ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();

    private final ConnectionManager connectionManager = ConnectionManager.getInstance();

    // ACK统计内部类
    private static class AckStats {
        private volatile long totalSent = 0;
        private volatile long successSent = 0;
        private volatile long failedSent = 0;
        private volatile long retrySent = 0;
        private volatile long lastSentTime = 0;

        public void recordSuccess() {
            totalSent++;
            successSent++;
            lastSentTime = System.currentTimeMillis();
        }

        public void recordFailure() {
            totalSent++;
            failedSent++;
        }

        public void recordRetry() {
            retrySent++;
        }

        @Override
        public String toString() {
            return String.format("AckStats{total=%d, success=%d, failed=%d, retry=%d, lastSent=%d}",
                    totalSent, successSent, failedSent, retrySent, lastSentTime);
        }
    }

    public DefaultBrokerApplicationProtocolHandler(TopicsManager topicsManager, SeqManager seqManager) {
        this.topicsManager = topicsManager;
        this.seqManager = seqManager;

        // 启动心跳超时检测任务，每30秒检查一次
        heartbeatExecutor.scheduleAtFixedRate(this::checkHeartbeatTimeout, 30, 30, TimeUnit.SECONDS);

        // 添加JVM关闭钩子，确保资源释放
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    @Override
    public void dispatch(ChannelHandlerContext ctx, TransportMessage msg) {
        try {
            Channel channel = ctx.channel();
            switch (msg.getType()) {
                case ACK -> onHandleAck(ctx, msg);
                case MESSAGE -> onHandleMessage(ctx, msg);
                case HEARTBEAT -> onHeartbeat(ctx, msg);
                case CONNECT -> onConnect(ctx, msg);
                case DISCONNECT -> onDisconnect(ctx, msg);
                case POLL_REQUEST -> onPoll(ctx, msg);
                default -> log.warn("Received unknown message type: {}", msg.getType());
            }
        } catch (Exception e) {
            log.error("Error handling message: ", e);
        }
    }

    public void onSendMessage(ChannelHandlerContext ctx, TransportMessage msg) {
        Channel channel = ctx.channel();
        channel.writeAndFlush(msg);
        log.debug("Sent message from server: type={}, seq={}", msg.getType(), msg.getSeq());
    }

    public void onHandleMessage(ChannelHandlerContext ctx, TransportMessage msg) {
        try {
            Channel channel = ctx.channel();

            // Check if client is registered (has sent CONNECT message)
            Connection connection = connectionManager.getConnection(channel);
            if (connection == null) {
                log.warn("Received message from unregistered client: {}", channel.remoteAddress());
                // Send ACK anyway to avoid client hanging
                sendAck(channel, msg, false, "Client not registered");
                return;
            }

            // Validate message format
            if (!isValidMessage(msg)) {
                log.warn("Invalid message format from client {}: seq={}", connection.getClientId(), msg.getSeq());
                sendAck(channel, msg, false, "Invalid message format");
                return;
            }

            long msgSeq = msg.getSeq();
            String clientId = connection.getClientId();

            // Determine if it is a duplicate message
            if (seqManager.checkAndUpdateSeq(clientId, msgSeq)) {
                // If it is a new message, publish it to the topic
                MessagePack messagePack = msg.getMessagePack();
                topicsManager.publish(messagePack);
                log.debug("Processed message from client {}: seq={}", clientId, msgSeq);
            } else {
                // If it is a duplicate message, Respond with ACK directly
                log.debug("Duplicated message from client {}: seq={}", clientId, msgSeq);
            }

            // Respond with ACK
            sendAck(channel, msg, true, null);
        } catch (Exception e) {
            log.error("Error processing message: ", e);
            sendAck(ctx.channel(), msg, false, "Internal server error: " + e.getMessage());
        }
    }

    public void onSendAck(ChannelHandlerContext ctx, TransportMessage msg) {
        // Server doesn't dispatch ACK messages from clients
    }

    public void onHandleAck(ChannelHandlerContext ctx, TransportMessage msg) {
        // Server doesn't dispatch ACK messages from clients
    }

    public void onPoll(ChannelHandlerContext ctx, TransportMessage msg) {
        Channel channel = ctx.channel();

        // Ensure client is registered
        Connection connection = connectionManager.getConnection(channel);
        if (connection == null) {
            log.warn("Received poll from unregistered client: {}", channel.remoteAddress());
            sendAck(channel, msg, false, "Client not registered");
            return;
        }

        connection.updateHeartbeat();

        MessagePack next = fetchNextMessage(msg);

        TransportMessage.Builder builder = TransportMessage.newBuilder()
                .setType(MessageType.POLL_RESPONSE)
                .setSeq(msg != null ? msg.getSeq() : 0)
                .setTimestamp(System.currentTimeMillis());

        if (next != null) {
            builder.setMessagePack(next);
        }

        channel.writeAndFlush(builder.build());
    }

    public void onHeartbeat(ChannelHandlerContext ctx, TransportMessage msg) {
        Channel channel = ctx.channel();

        // 检查客户端是否已注册（已发送CONNECT消息）
        Connection connection = connectionManager.getConnection(channel);
        if (connection == null) {
            log.warn("Received heartbeat from unregistered client: {}", channel.remoteAddress());
            // 不发送心跳响应，因为客户端未注册
            return;
        }

        // 更新心跳时间
        connection.updateHeartbeat();

        // Respond with heartbeat ACK to keep connection alive
        TransportMessage heartbeatAck = TransportMessage.newBuilder()
                .setType(MessageType.HEARTBEAT)
                .setSeq(msg.getSeq())
                .setTimestamp(System.currentTimeMillis())
                .build();

        channel.writeAndFlush(heartbeatAck);
        log.debug("Sent heartbeat response to client {}", channel.remoteAddress());
    }

    public void onConnect(ChannelHandlerContext ctx, TransportMessage msg) {
        Channel channel = ctx.channel();
        
        try {
            String clientId;
            // 如果消息为null或没有CONNECT负载，创建一个默认的客户端ID
            if (msg == null || !msg.hasConnect()) {
                clientId = "client-" + channel.id().asShortText();
            } else {
                // 处理正常的CONNECT消息
                clientId = msg.getConnect().getClientId();
            }

            // Validate client ID
            if (clientId == null || clientId.trim().isEmpty()) {
                log.warn("Connection rejected: empty client ID from {}", channel.remoteAddress());
                sendAck(channel, msg, false, "Invalid client ID");
                return;
            }

            // Check if client already connected
            if (connectionManager.getConnection(clientId) != null) {
                log.warn("Connection rejected: client {} already connected", clientId);
                sendAck(channel, msg, false, "Client already connected");
                return;
            }

            // Register client
            seqManager.registerClient(clientId);
            
            // 创建连接对象
            connectionManager.createConnection(ctx, clientId);

            // Send successful ACK
            sendAck(channel, msg, true, null);

            log.info("Connection established with client {} from {}", clientId, channel.remoteAddress());
        } catch (Exception e) {
            log.error("Error handling connection from {}: ", channel.remoteAddress(), e);
            sendAck(channel, msg, false, "Connection error: " + e.getMessage());
        }
    }

    public void onDisconnect(ChannelHandlerContext ctx, TransportMessage msg) {
        Channel channel = ctx.channel();
        Connection connection = connectionManager.getConnection(channel);

        if (connection != null) {
            String clientId = connection.getClientId();
            seqManager.removeClient(clientId);
            
            // 关闭连接
            connectionManager.closeConnection(channel);
            
            if (msg != null) {
                // Client sent DISCONNECT message
                log.info("Client {} disconnected gracefully, duration: {}ms",
                        clientId, connection.getConnectionDuration());
            } else {
                // Connection lost without DISCONNECT message
                log.info("Client {} connection lost, duration: {}ms",
                        clientId, connection.getConnectionDuration());
            }
        } else {
            if (msg != null) {
                log.info("Unregistered client from {} disconnected gracefully", channel.remoteAddress());
            } else {
                log.info("Unregistered client from {} connection lost", channel.remoteAddress());
            }
        }
        
        // 不再发送ACK响应，因为连接已经关闭
        // 如果需要，可以在这里添加其他清理逻辑
    }

    // Server主动断开连接
    public void disconnectClient(Channel channel) {
        if (channel != null && channel.isActive()) {
            Connection connection = connectionManager.getConnection(channel);
            if (connection != null) {
                // Send DISCONNECT message
                connection.sendDisconnect();
                
                // Close the channel
                connection.close();
                
                log.info("Disconnected client {}", connection.getClientId());
            }
        }
    }

    private MessagePack fetchNextMessage(TransportMessage request) {
        List<String> requestedTopics = (request != null && request.hasPollRequest())
                ? request.getPollRequest().getTopicsList()
                : List.of();
        return topicsManager.poll(requestedTopics);
    }

    private void sendAck(Channel channel, TransportMessage originalMsg, boolean success, String errorMessage) {
        sendAckWithRetry(channel, originalMsg, success, errorMessage, 0);
    }

    /**
     * 发送ACK响应，带有重试机制
     */
    private void sendAckWithRetry(Channel channel, TransportMessage originalMsg, boolean success, String errorMessage, int retryCount) {
        if (channel == null || !channel.isActive()) {
            log.warn("Cannot send ACK, channel is not active for client {}",
                    channel != null ? channel.remoteAddress() : "null");
            return;
        }

        // 获取客户端ID用于统计
        Connection connection = connectionManager.getConnection(channel);
        String clientId = connection != null ? connection.getClientId() : "unknown";

        // 获取或创建ACK统计对象
        AckStats stats = ackStatsMap.computeIfAbsent(clientId, k -> new AckStats());

        // 处理null的originalMsg，使用默认值
        long seq = (originalMsg != null) ? originalMsg.getSeq() : 0;
        
        AckPayload.Builder ackBuilder = AckPayload.newBuilder()
                .setSuccess(success)
                .setAckMessageId((int) seq);

        if (!success && errorMessage != null) {
            ackBuilder.setErrorMessage(errorMessage);
        }

        TransportMessage ackMsg = TransportMessage.newBuilder()
                .setType(MessageType.ACK)
                .setSeq(seq)
                .setTimestamp(System.currentTimeMillis())
                .setAck(ackBuilder.build())
                .build();

        // 异步发送ACK
        channel.writeAndFlush(ackMsg).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                log.debug("Sent ACK to client {} for msg seq={}, success={}",
                        channel.remoteAddress(), seq, success);
                stats.recordSuccess();
            } else {
                log.error("Failed to send ACK to client {} for msg seq={}, retryCount={}",
                        channel.remoteAddress(), seq, retryCount, future.cause());

                // 重试逻辑，最多重试2次
                if (retryCount < brokerConfig.getAckMaxRetries()) {
                    stats.recordRetry();
                    // 延迟重试
                    channel.eventLoop().schedule(() -> {
                        sendAckWithRetry(channel, originalMsg, success, errorMessage, retryCount + 1);
                    }, brokerConfig.getAckRetryDelayMs() * (retryCount + 1), TimeUnit.MILLISECONDS);
                } else {
                    int attempts = brokerConfig.getAckMaxRetries() + 1;
                    log.error("Failed to send ACK after {} attempts to client {} for msg seq={}",
                            attempts, channel.remoteAddress(), seq);
                    stats.recordFailure();
                }
            }
        });
    }

    /**
     * Validates the format of a received message
     */
    private boolean isValidMessage(TransportMessage msg) {
        if (msg == null) {
            return false;
        }

        // Check if the message has the required fields
        if (msg.getType() != MessageType.MESSAGE) {
            return false;
        }

        // Allow sequence numbers >= 1 (CONNECT messages use seq=0)
        if (msg.getSeq() < 1) {
            return false;
        }

        // Check if the message pack is present
        if (!msg.hasMessagePack()) {
            return false;
        }

        MessagePack messagePack = msg.getMessagePack();

        // Check if topics list is not empty (allow empty for now)
        // This can be enhanced to use a default topic if needed
        // if (messagePack.getTopicsCount() == 0) {
        //     return false;
        // }

        // Check if message is present
        if (!messagePack.hasMessage()) {
            return false;
        }

        Message message = messagePack.getMessage();

        // Check if message has valid ID
        if (message.getMessageId() <= 0) {
            return false;
        }

        return true;
    }

    /**
     * 获取ACK统计信息
     */
    public Map<String, String> getAckStats() {
        Map<String, String> result = new HashMap<>();
        ackStatsMap.forEach((clientId, stats) -> {
            result.put(clientId, stats.toString());
        });
        return result;
    }

    /**
     * 重置ACK统计信息
     */
    public void resetAckStats() {
        ackStatsMap.clear();
        log.info("ACK statistics reset");
    }

    /**
     * 获取指定客户端的ACK统计信息
     */
    public AckStats getClientAckStats(String clientId) {
        return ackStatsMap.get(clientId);
    }

    /**
     * 检查心跳超时的连接
     */
    private void checkHeartbeatTimeout() {
        List<Connection> timeoutConnections = new ArrayList<>();

        // 遍历所有连接，检查心跳超时
        long timeoutMs = brokerConfig.getHeartbeatTimeoutMs();
        for (Connection connection : connectionManager.getAllConnections().values()) {
            if (connection.isHeartbeatTimeout(timeoutMs)) {
                timeoutConnections.add(connection);
            }
        }

        // 处理超时的连接
        for (Connection connection : timeoutConnections) {
            String clientId = connection.getClientId();
            Channel channel = connection.getChannel();
            log.warn("Client {} heartbeat timeout, closing connection", clientId);

            // 断开连接
            disconnectClient(channel);
        }

        if (!timeoutConnections.isEmpty()) {
            log.info("Heartbeat timeout check completed, disconnected {} clients", timeoutConnections.size());
        }
    }

    /**
     * 关闭资源
     */
    public void shutdown() {
        log.info("Shutting down DefaultBrokerApplicationProtocolHandler");

        // 关闭心跳检测定时器
        heartbeatExecutor.shutdown();
        try {
            if (!heartbeatExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                heartbeatExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            heartbeatExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // 断开所有客户端连接
        for (Connection connection : connectionManager.getAllConnections().values()) {
            if (connection.isActive() && connection.getChannel().isActive()) {
                disconnectClient(connection.getChannel());
            }
        }

        log.info("DefaultBrokerApplicationProtocolHandler shutdown completed");
    }

    /**
     * 处理连接异常
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        Channel channel = ctx.channel();
        Connection connection = connectionManager.getConnection(channel);
        String clientId = connection != null ? connection.getClientId() : "unknown";

        if (cause instanceof java.io.IOException) {
            // 网络IO异常，通常是客户端断开连接
            log.info("Network exception with client {}: {}", clientId, cause.getMessage());
        } else {
            // 其他异常
            log.error("Exception with client {}: ", clientId, cause);
        }

        // 关闭连接
        ctx.close();
    }

    /**
     * 处理连接空闲事件
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;
            Channel channel = ctx.channel();
            Connection connection = connectionManager.getConnection(channel);
            String clientId = connection != null ? connection.getClientId() : "unknown";

            if (event.state() == IdleState.READER_IDLE) {
                log.warn("Client {} idle for too long, closing connection", clientId);
                ctx.close();
            } else if (event.state() == IdleState.WRITER_IDLE) {
                log.debug("Client {} writer idle, sending heartbeat", clientId);
                // 发送心跳
                TransportMessage heartbeatMsg = TransportMessage.newBuilder()
                        .setType(MessageType.HEARTBEAT)
                        .setTimestamp(System.currentTimeMillis())
                        .build();
                channel.writeAndFlush(heartbeatMsg);
            }
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    /**
     * 处理连接关闭事件
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        Connection connection = connectionManager.getConnection(channel);

        if (connection != null) {
            String clientId = connection.getClientId();
            log.info("Client {} connection inactive", clientId);

            // 清理连接信息
            seqManager.removeClient(clientId);
            connectionManager.closeConnection(channel);
            
            log.info("Connection closed with client {}, duration: {}ms",
                    clientId, connection.getConnectionDuration());
        }

        super.channelInactive(ctx);
    }

    /**
     * 获取连接统计信息
     */
    public Map<String, String> getConnectionStats() {
        Map<String, String> result = new HashMap<>();
        connectionManager.getAllConnections().forEach((channel, connection) -> {
            String clientId = connection.getClientId();
            String stats = String.format("Connection{clientId=%s, active=%s, lastHeartbeat=%d, duration=%d}",
                    clientId, connection.isActive(), connection.getLastHeartbeatTime(), connection.getConnectionDuration());
            result.put(clientId, stats);
        });
        return result;
    }

    /**
     * 获取连接总数
     */
    public int getActiveConnectionCount() {
        return connectionManager.getConnectionCount();
    }

    /**
     * 获取所有ACK统计的总览
     */
    public String getAckStatsSummary() {
        long totalSent = ackStatsMap.values().stream().mapToLong(s -> s.totalSent).sum();
        long successSent = ackStatsMap.values().stream().mapToLong(s -> s.successSent).sum();
        long failedSent = ackStatsMap.values().stream().mapToLong(s -> s.failedSent).sum();
        long retrySent = ackStatsMap.values().stream().mapToLong(s -> s.retrySent).sum();

        return String.format("Total ACK Stats: sent=%d, success=%d, failed=%d, retry=%d, successRate=%.2f%%",
                totalSent, successSent, failedSent, retrySent,
                totalSent > 0 ? (successSent * 100.0 / totalSent) : 0.0);
    }
}
