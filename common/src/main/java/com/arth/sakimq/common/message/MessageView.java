package com.arth.sakimq.common.message;

public class MessageView {
    private final ReusableMessageEvent event;
    private final long offset;
    public MessageView(ReusableMessageEvent event, long offset) {
        this.event = event;
        this.offset = offset;
    }
    // 提供只读 getter，body 返回 copy 或 slice
    public byte[] getBody() {
        return Arrays.copyOf(event.getBody(), event.getBodyLen());
    }
}
