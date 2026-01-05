package com.arth.sakimq.broker.core.impl;

import com.arth.sakimq.broker.config.BrokerConfig;
import com.arth.sakimq.broker.log.MessageLogWriter;
import com.arth.sakimq.broker.seq.SeqManager;
import com.arth.sakimq.broker.store.MessageStore;
import com.arth.sakimq.broker.store.OffsetManager;
import com.arth.sakimq.broker.topic.TopicsManager;
import com.arth.sakimq.common.constant.LoggerName;
import com.arth.sakimq.network.handler.BrokerProtocolHandler;
import com.arth.sakimq.network.netty.Connection;
import com.arth.sakimq.network.netty.ConnectionManager;
import com.arth.sakimq.protocol.*;
import com.arth.sakimq.protocol.AckPayload;
import com.arth.sakimq.protocol.Message;
import com.arth.sakimq.protocol.MessagePack;
import com.arth.sakimq.protocol.MessageType;
import com.arth.sakimq.protocol.OffsetCommitPayload;
import com.arth.sakimq.protocol.PollRequest;
import com.arth.sakimq.protocol.PollResponse;
import com.arth.sakimq.protocol.TransportMessage;
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
    private final MessageLogWriter messageLogWriter;
    // ACK send statistics
    private final ConcurrentMap<String, AckStats> ackStatsMap = new ConcurrentHashMap<>();

    // Heartbeat timeout checker
    private final ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();

    private final ConnectionManager connectionManager = ConnectionManager.getInstance();

    // Internal ACK stats holder
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

    private final MessageStore messageStore;
    private final OffsetManager offsetManager;

    public DefaultBrokerApplicationProtocolHandler(TopicsManager topicsManager, SeqManager seqManager, MessageStore messageStore, OffsetManager offsetManager) {
        this.topicsManager = topicsManager;
        this.seqManager = seqManager;
        this.messageStore = messageStore;
        this.offsetManager = offsetManager;
        this.messageLogWriter = null; // Deprecated

        // Start heartbeat timeout checks every 30s
        heartbeatExecutor.scheduleAtFixedRate(this::checkHeartbeatTimeout, 30, 30, TimeUnit.SECONDS);

        // Add JVM shutdown hook to release resources
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
                case OFFSET_COMMIT -> onOffsetCommit(ctx, msg);
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

                if (messageStore != null) {
                    try {
                        messageStore.append(messagePack);
                    } catch (Exception e) {
                        log.error("Persist message to store failed, rejecting message seq {} from {}: {}", msgSeq, clientId, e.getMessage());
                        sendAck(channel, msg, false, "Message persistence failed");
                        return;
                    }
                }

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

        if (!msg.hasPollRequest()) {
            sendAck(channel, msg, false, "Invalid Poll Request");
            return;
        }

        PollRequest request = msg.getPollRequest();
        String groupId = request.getGroupId();
        if (groupId == null || groupId.isEmpty()) {
            // Default group if not provided
            groupId = "default";
        }
        List<String> topics = request.getTopicsList();

        MessagePack foundMsg = null;
        long foundOffset = -1;
        String foundTopic = null;

        for (String topicName : topics) {
            com.arth.sakimq.broker.topic.Topic topic = topicsManager.getTopic(topicName);
            if (topic == null) continue;

            long offset = offsetManager.getOffset(groupId, topicName);
            if (offset < 0) {
                offset = 0; // Start from beginning by default
            }

            MessagePack mp = topic.getMessage(offset);
            if (mp != null) {
                foundMsg = mp;
                foundOffset = offset;
                foundTopic = topicName;
                break; // Found a message, break and return
            }
        }

        TransportMessage.Builder builder = TransportMessage.newBuilder()
                .setType(MessageType.POLL_RESPONSE)
                .setSeq(msg.getSeq())
                .setTimestamp(System.currentTimeMillis());

        PollResponse.Builder pollResponseBuilder = PollResponse.newBuilder();
        if (foundMsg != null) {
            pollResponseBuilder.setMessagePack(foundMsg);
            pollResponseBuilder.setOffset(foundOffset);
            pollResponseBuilder.setTopic(foundTopic);
        }

        builder.setPollResponse(pollResponseBuilder);
        channel.writeAndFlush(builder.build());
    }

    public void onOffsetCommit(ChannelHandlerContext ctx, TransportMessage msg) {
        if (!msg.hasOffsetCommit()) {
             sendAck(ctx.channel(), msg, false, "Invalid Offset Commit");
             return;
        }
        OffsetCommitPayload commit = msg.getOffsetCommit();
        offsetManager.updateOffset(commit.getGroupId(), commit.getTopic(), commit.getOffset());
        // Auto persist is handled inside OffsetManager.updateOffset
        sendAck(ctx.channel(), msg, true, null);
    }

    public void onHeartbeat(ChannelHandlerContext ctx, TransportMessage msg) {
        Channel channel = ctx.channel();

        // Ensure client is registered (sent CONNECT)
        Connection connection = connectionManager.getConnection(channel);
        if (connection == null) {
            log.warn("Received heartbeat from unregistered client: {}", channel.remoteAddress());
            // Skip heartbeat response because client is not registered
            return;
        }

        // Refresh heartbeat timestamp
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
            // If msg is null or lacks CONNECT payload, generate a default client id
            if (msg == null || !msg.hasConnect()) {
                clientId = "client-" + channel.id().asShortText();
            } else {
                // Normal CONNECT payload, extract client id
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
            
            // Create connection wrapper
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
            
            // Close channel
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
        
        // Skip ACK because connection is closed; add more cleanup here if needed
    }

    // Server-initiated disconnect
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


    private void sendAck(Channel channel, TransportMessage originalMsg, boolean success, String errorMessage) {
        sendAckWithRetry(channel, originalMsg, success, errorMessage, 0);
    }

    /**
     * Send ACK with retry.
     */
    private void sendAckWithRetry(Channel channel, TransportMessage originalMsg, boolean success, String errorMessage, int retryCount) {
        if (channel == null || !channel.isActive()) {
            log.warn("Cannot send ACK, channel is not active for client {}",
                    channel != null ? channel.remoteAddress() : "null");
            return;
        }

        // Client id for stats
        Connection connection = connectionManager.getConnection(channel);
        String clientId = connection != null ? connection.getClientId() : "unknown";

        // Get or create ACK stats holder
        AckStats stats = ackStatsMap.computeIfAbsent(clientId, k -> new AckStats());

        // Handle null originalMsg with default seq
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

        // Async send ACK
        channel.writeAndFlush(ackMsg).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                log.debug("Sent ACK to client {} for msg seq={}, success={}",
                        channel.remoteAddress(), seq, success);
                stats.recordSuccess();
            } else {
                log.error("Failed to send ACK to client {} for msg seq={}, retryCount={}",
                        channel.remoteAddress(), seq, retryCount, future.cause());

                // Retry until max attempts
                if (retryCount < brokerConfig.getAckMaxRetries()) {
                    stats.recordRetry();
                    // Backoff before retry
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
     * Get ACK stats snapshot.
     */
    public Map<String, String> getAckStats() {
        Map<String, String> result = new HashMap<>();
        ackStatsMap.forEach((clientId, stats) -> {
            result.put(clientId, stats.toString());
        });
        return result;
    }

    /**
     * Reset ACK stats.
     */
    public void resetAckStats() {
        ackStatsMap.clear();
        log.info("ACK statistics reset");
    }

    /**
     * Get ACK stats for a single client.
     */
    public AckStats getClientAckStats(String clientId) {
        return ackStatsMap.get(clientId);
    }

    /**
     * Check connections that exceeded heartbeat timeout.
     */
    private void checkHeartbeatTimeout() {
        List<Connection> timeoutConnections = new ArrayList<>();

        // Scan all connections for heartbeat timeout
        long timeoutMs = brokerConfig.getHeartbeatTimeoutMs();
        for (Connection connection : connectionManager.getAllConnections().values()) {
            if (connection.isHeartbeatTimeout(timeoutMs)) {
                timeoutConnections.add(connection);
            }
        }

        // Disconnect timed-out connections
        for (Connection connection : timeoutConnections) {
            String clientId = connection.getClientId();
            Channel channel = connection.getChannel();
            log.warn("Client {} heartbeat timeout, closing connection", clientId);

            // Close connection
            disconnectClient(channel);
        }

        if (!timeoutConnections.isEmpty()) {
            log.info("Heartbeat timeout check completed, disconnected {} clients", timeoutConnections.size());
        }
    }

    /**
     * Close resources.
     */
    public void shutdown() {
        log.info("Shutting down DefaultBrokerApplicationProtocolHandler");

        // Stop heartbeat scheduler
        heartbeatExecutor.shutdown();
        try {
            if (!heartbeatExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                heartbeatExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            heartbeatExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        if (messageLogWriter != null) {
            try {
                messageLogWriter.close();
            } catch (Exception e) {
                log.warn("Failed to close message log writer cleanly: {}", e.getMessage());
            }
        }

        // Disconnect all clients
        for (Connection connection : connectionManager.getAllConnections().values()) {
            if (connection.isActive() && connection.getChannel().isActive()) {
                disconnectClient(connection.getChannel());
            }
        }

        log.info("DefaultBrokerApplicationProtocolHandler shutdown completed");
    }

    /**
     * Handle channel exceptions.
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        Channel channel = ctx.channel();
        Connection connection = connectionManager.getConnection(channel);
        String clientId = connection != null ? connection.getClientId() : "unknown";

        if (cause instanceof java.io.IOException) {
            // Network IO usually means client disconnected
            log.info("Network exception with client {}: {}", clientId, cause.getMessage());
        } else {
            // Other exceptions
            log.error("Exception with client {}: ", clientId, cause);
        }

        // Close channel
        ctx.close();
    }

    /**
     * Handle idle events.
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
                // Send heartbeat
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
     * Handle channel inactive.
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        Connection connection = connectionManager.getConnection(channel);

        if (connection != null) {
            String clientId = connection.getClientId();
            log.info("Client {} connection inactive", clientId);

            // Cleanup connection info
            seqManager.removeClient(clientId);
            connectionManager.closeConnection(channel);
            
            log.info("Connection closed with client {}, duration: {}ms",
                    clientId, connection.getConnectionDuration());
        }

        super.channelInactive(ctx);
    }

    /**
     * Get connection stats summary.
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
     * @return active connection count
     */
    public int getActiveConnectionCount() {
        return connectionManager.getConnectionCount();
    }

    /**
     * Get aggregate ACK stats summary.
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
