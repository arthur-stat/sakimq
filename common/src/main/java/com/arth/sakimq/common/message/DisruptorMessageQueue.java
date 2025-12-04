package com.arth.sakimq.common.message;

import com.arth.sakimq.common.constant.LoggerName;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DisruptorMessageQueue implements MessageQueue {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.MESSAGE);
    private final RingBuffer<ReusableMessageEvent> ringBuffer;
    private final Disruptor<ReusableMessageEvent> disruptor;

    public DisruptorMessageQueue(int bufferSize, int consumerGroupSize, MessageHandler handler) {

        this.disruptor = new Disruptor<>(
                ReusableMessageEvent.FACTORY,
                bufferSize,
                Executors.defaultThreadFactory(),
                ProducerType.MULTI,
                new TimeoutBlockingWaitStrategy(5, TimeUnit.MINUTES)
        );

        /* WorkHandler API: Competitive Consumption */
        WorkHandler<ReusableMessageEvent>[] workers = new WorkHandler[consumerGroupSize];

        Arrays.fill(workers, (WorkHandler<ReusableMessageEvent>) event -> {
            try {
                /* PUSH mode */
                handler.onMessage(event);
            } catch (Exception e) {
                log.error("Error processing message", e);
                // TODO: retry or dead-letter
            }
        });

        /* Thanks to Disruptor 3.4.4, this single line of code implements the push mode
        of the message queue, saving me a tremendous amount of work. */
        disruptor.handleEventsWithWorkerPool(workers);
        this.ringBuffer = disruptor.start();
    }

    /**
     * append (push)
     */
    @Override
    public boolean append(Message message) {
        try {
            long seq = ringBuffer.tryNext();
            try {
                ReusableMessageEvent event = ringBuffer.get(seq);
                event.reset(
                        message.getMessageId(),
                        message.getTopic(),
                        0,
                        message.getBody(),
                        message.getTimestamp()
                );
            } finally {
                ringBuffer.publish(seq);
            }
            return true;

        } catch (InsufficientCapacityException e) {
            log.warn("Disruptor queue full");
            return false;
        }
    }

    /**
     * blocking append (push)
     */
    @Override
    public void appendBlocking(Message message) {
        long seq = ringBuffer.next();
        try {
            ReusableMessageEvent event = ringBuffer.get(seq);
            event.reset(
                    message.getMessageId(),
                    message.getTopic(),
                    0,
                    message.getBody(),
                    message.getTimestamp()
            );
        } finally {
            ringBuffer.publish(seq);
        }
    }

    @Override
    public boolean isFull() {
        return ringBuffer.remainingCapacity() == 0;
    }

    @Override
    public void close() {
        disruptor.shutdown();
    }
}
