package com.arth.sakimq.common.message;

public class DisruptorMessageQueue {

    private final RingBuffer<ReusableMessageEvent> ringBuffer;
    private final SequenceBarrier barrier;
    private final Sequence consumedSequence = new Sequence(-1); // 消费到哪了
    private final AtomicLong committedOffset = new AtomicLong(-1); // 已提交 offset
    private final ExecutorService consumerExecutor;
    private final MessageHandler messageHandler; // 回调给上层消费逻辑

    // 内部缓存：已处理但未提交的消息（用于 pull）
    private final Map<Long, ReusableMessageEvent> processedMessages = new ConcurrentHashMap<>();

    public PartitionQueue(int bufferSize, MessageHandler handler) {
        this.messageHandler = handler;
        Disruptor<ReusableMessageEvent> disruptor = new Disruptor<>(
                ReusableMessageEvent.FACTORY,
                bufferSize,
                Executors.defaultThreadFactory(),
                ProducerType.SINGLE, // 根据场景选 SINGLE/MULTI
                new BlockingWaitStrategy() // 可换为 Sleeping/LowLatency
        );

        // 消费者：处理消息并缓存
        disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
            try {
                messageHandler.onMessage(event); // 上层处理逻辑
                processedMessages.put(sequence, event); // 标记为已处理
                consumedSequence.set(sequence);
            } catch (Exception e) {
                // TODO: 死信队列 or 重试
                consumedSequence.set(sequence); // 跳过？谨慎！
            }
        });

        this.ringBuffer = disruptor.start();
        this.barrier = ringBuffer.newBarrier();
        this.consumerExecutor = Executors.newSingleThreadExecutor();
    }

    // ====== 入队======
    @Override
    public boolean append(Message msg) {
        try {
            long seq = ringBuffer.tryNext(); // 非阻塞
            ringBuffer.get(seq).reset(
                    msg.getMessageId(),
                    msg.getTopic(),
                    0, // queueId
                    msg.getBody(),
                    msg.getTimestamp()
            );
            ringBuffer.publish(seq);
            return true;
        } catch (InsufficientCapacityException e) {
            return false; // 队列满，触发背压
        }
    }

    // ====== 拉取======
    @Override
    public FetchResult pull(long startOffset, int maxMessages) {
        List<MessageView> messages = new ArrayList<>();
        long nextOffset = startOffset;

        for (int i = 0; i < maxMessages; i++) {
            if (nextOffset > consumedSequence.get()) break; // 还没处理完

            ReusableMessageEvent event = processedMessages.get(nextOffset);
            if (event == null) continue; // 可能被清理

            messages.add(new MessageView(event, nextOffset));
            nextOffset++;
        }

        // TODO: 定期清理 processedMessages（如 <= committedOffset 的条目）

        return new FetchResult(messages, nextOffset, nextOffset <= getMaxOffset());
    }

    // ====== 提交消费位点（FIFO 保证）======
    @Override
    public void commitOffset(long offset) {
        if (offset > committedOffset.get() && offset <= consumedSequence.get()) {
            committedOffset.set(offset);
            // 清理已提交的消息（节省内存）
            processedMessages.entrySet().removeIf(e -> e.getKey() <= offset);
        }
    }

    // ====== 状态查询 ======
    @Override
    public long getMaxOffset() {
        return ringBuffer.getCursor(); // 最新写入位置
    }

    @Override
    public long getCommittedOffset() {
        return committedOffset.get();
    }

    @Override
    public boolean isFull() {
        return ringBuffer.remainingCapacity() == 0;
    }

    // ====== 生命周期 ======
    @Override
    public void close() {
        // Disruptor shutdown logic
    }
}
}
