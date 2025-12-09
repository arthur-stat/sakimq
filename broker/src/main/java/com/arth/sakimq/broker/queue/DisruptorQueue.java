package com.arth.sakimq.broker.queue;

import com.arth.sakimq.broker.config.QueueConfig;
import com.arth.sakimq.common.constant.LoggerName;
import com.arth.sakimq.protocol.Message;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Disruptor-backed message queue (Pull mode)
 */
public class DisruptorQueue implements PullQueue {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.MESSAGE);

    private int queueId;
    private final RingBuffer<ReusableMessageEvent> ringBuffer;
    private final SequenceBarrier barrier;
    private final Sequence consumerSequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
    private final Disruptor<ReusableMessageEvent> disruptor;

    public DisruptorQueue(QueueConfig config) {
        this.disruptor = new Disruptor<>(
                ReusableMessageEvent.FACTORY,
                config.getBufferSize(),
                Executors.defaultThreadFactory(),
                ProducerType.MULTI,
                new TimeoutBlockingWaitStrategy(config.getTimeout(), TimeUnit.MINUTES)
        );

        this.ringBuffer = disruptor.start();
        this.barrier = ringBuffer.newBarrier();
    }

    @Override
    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    /**
     * Producer appends message to queue
     */
    @Override
    public boolean append(Message message) {
        try {
            long seq = ringBuffer.tryNext();
            try {
                ReusableMessageEvent event = ringBuffer.get(seq);
                event.reset(queueId, message);
            } finally {
                ringBuffer.publish(seq);
            }
            return true;
        } catch (InsufficientCapacityException e) {
            log.warn("Disruptor queue full");
            return false;
        }
    }

    @Override
    public void appendBlocking(Message message) {
        long seq = ringBuffer.next();
        try {
            ReusableMessageEvent event = ringBuffer.get(seq);
            event.reset(queueId, message);
        } finally {
            ringBuffer.publish(seq);
        }
    }

    /**
     * Non-blocking poll
     */
    @Override
    public Message poll() {
        long nextSeq = consumerSequence.get() + 1;
        if (nextSeq > ringBuffer.getCursor()) return null;
        ReusableMessageEvent event = ringBuffer.get(nextSeq);
        consumerSequence.set(nextSeq);
        return event.getMessage();
    }

    /**
     * Blocking take
     */
    @Override
    public Message take() {
        try {
            long nextSeq = consumerSequence.get() + 1;
            long availableSeq = barrier.waitFor(nextSeq);
            ReusableMessageEvent event = ringBuffer.get(nextSeq);
            consumerSequence.set(nextSeq);
            return event.getMessage();
        } catch (AlertException | InterruptedException e) {
            log.warn("Disruptor queue take interrupted");
            Thread.currentThread().interrupt();
            return null;
        } catch (TimeoutException e) {
            log.warn("Disruptor queue take timeout");
            return null;
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
