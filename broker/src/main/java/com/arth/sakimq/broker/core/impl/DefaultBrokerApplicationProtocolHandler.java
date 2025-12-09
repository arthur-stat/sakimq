package com.arth.sakimq.broker.core.impl;

import com.arth.sakimq.broker.seq.SeqManager;
import com.arth.sakimq.common.constant.LoggerName;
import com.arth.sakimq.broker.topic.TopicsManager;
import com.arth.sakimq.network.handler.BrokerProtocolHandler;
import com.arth.sakimq.network.handler.ClientProtocolHandler;
import com.arth.sakimq.protocol.AckPayload;
import com.arth.sakimq.protocol.MessageType;
import com.arth.sakimq.protocol.TransportMessage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DefaultBrokerApplicationProtocolHandler implements BrokerProtocolHandler {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER);
    private final TopicsManager topicsManager;
    private final SeqManager seqManager;
    private final ConcurrentMap<Channel, String> channelToClientId = new ConcurrentHashMap<>();

    public DefaultBrokerApplicationProtocolHandler(TopicsManager topicsManager, SeqManager seqManager) {
        this.topicsManager = topicsManager;
        this.seqManager = seqManager;
    }

    @Override
    public void onHandleMessage(TransportMessage msg) {
        try {
            // Check if client is registered (has sent CONNECT message)
            String clientId = channelToClientId.get(channel);
            if (clientId == null) {
                log.warn("Received message from unregistered client: {}", channel.remoteAddress());
                // Send ACK anyway to avoid client hanging
                sendAck(channel, msg);
                return;
            }
            
            long msgSeq = msg.getSeq();
            
            // Determine if it is a duplicate message
            if (seqManager.checkAndUpdateSeq(clientId, msgSeq)) {
                // If it is a new message, publish it to the topic
                topicsManager.publish(msg.getMessagePack());
                log.debug("Processed message from client {}: seq={}", clientId, msgSeq);
            } else {
                // If it is a duplicate message, Respond with ACK directly
                log.debug("Duplicated message from client {}: seq={}", clientId, msgSeq);
            }
            
            // Respond with ACK
            sendAck(msg);
        } catch (Exception e) {
            log.error("Error processing message: ", e);
        }
    }

    @Override
    public void onMessage(TransportMessage msg) {
        // For server, onMessage is called when sending messages to clients
        channel.writeAndFlush(msg);
        log.debug("Sent message from server: type={}, seq={}", msg.getType(), msg.getSeq());
    }

    @Override
    public void onAck(Channel channel, TransportMessage msg) {
        // Server doesn't dispatch ACK messages from clients
    }

    @Override
    public void onHeartbeat(Channel channel, TransportMessage msg) {
        // TODO: Implement heartbeat handling
    }

    @Override
    public void onConnect(Channel channel, TransportMessage msg) {
        if (msg != null) {
            String clientId = msg.getConnect().getClientId();
            // Register client
            channelToClientId.put(channel, clientId);
            log.info("Connection established with client {}", clientId);
        }
    }

    @Override
    public void onDisconnect(TransportMessage msg) {
        String clientId = channelToClientId.remove(channel);
        if (clientId != null) {
            seqManager.removeClient(clientId);
            log.info("Connection closed with client {}", clientId);
        } else {
            log.info("Connection closed with unregistered client: {}", channel.remoteAddress());
        }
    }

    // Server主动断开连接
    public void disconnectClient(Channel channel) {
        if (channel != null && channel.isActive()) {
            // Send DISCONNECT message
            TransportMessage disconnectMsg = TransportMessage.newBuilder()
                    .setType(MessageType.DISCONNECT)
                    .setTimestamp(System.currentTimeMillis())
                    .build();
            
            // Send message and close channel
            channel.writeAndFlush(disconnectMsg).addListener(future -> {
                if (future.isSuccess()) {
                    log.info("Sent DISCONNECT to client {}", channel.remoteAddress());
                } else {
                    log.error("Failed to send DISCONNECT to client {}", channel.remoteAddress(), future.cause());
                }
                // Close the channel
                channel.close();
            });
        }
    }

    private void sendAck(Channel channel, TransportMessage originalMsg) {
        AckPayload ackPayload = AckPayload.newBuilder()
                .setSuccess(true)
                .setAckMessageId((int) originalMsg.getSeq())
                .build();
        
        TransportMessage ackMsg = TransportMessage.newBuilder()
                .setType(MessageType.ACK)
                .setSeq(originalMsg.getSeq())
                .setTimestamp(System.currentTimeMillis())
                .setAck(ackPayload)
                .build();
        
        channel.writeAndFlush(ackMsg);
        log.debug("Sent ACK to client {} for msg seq={}", channel.remoteAddress(), originalMsg.getSeq());
    }
}