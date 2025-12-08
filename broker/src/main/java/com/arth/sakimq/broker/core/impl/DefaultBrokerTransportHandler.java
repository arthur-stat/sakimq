package com.arth.sakimq.broker.core.impl;

import com.arth.sakimq.broker.seq.SeqManager;
import com.arth.sakimq.common.constant.LoggerName;
import com.arth.sakimq.broker.topic.TopicsManager;
import com.arth.sakimq.network.handler.TransportHandler;
import com.arth.sakimq.protocol.AckPayload;
import com.arth.sakimq.protocol.MessageType;
import com.arth.sakimq.protocol.TransportMessage;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultBrokerTransportHandler implements TransportHandler {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER);
    private final TopicsManager topicsManager;
    private final SeqManager seqManager;

    public DefaultBrokerTransportHandler(TopicsManager topicsManager, SeqManager seqManager) {
        this.topicsManager = topicsManager;
        this.seqManager = seqManager;
    }

    @Override
    public void onMessage(Channel channel, TransportMessage msg) {
        try {
            String clientId = channel.remoteAddress().toString();
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
            sendAck(channel, msg);
        } catch (Exception e) {
            log.error("Error processing message: ", e);
        }
    }

    @Override
    public void onAck(Channel channel, TransportMessage msg) {

    }

    @Override
    public void onHeartbeat(Channel channel, TransportMessage msg) {

    }

    @Override
    public void onConnect(Channel channel, TransportMessage msg) {
        String clientId = msg.getConnect().getClientId();
        log.info("Connection established with client {}", clientId);
    }

    @Override
    public void onDisconnect(Channel channel, TransportMessage msg) {
        String clientId = channel.remoteAddress().toString();
        seqManager.removeClient(clientId);
        log.info("Connection closed with client {}", clientId);
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