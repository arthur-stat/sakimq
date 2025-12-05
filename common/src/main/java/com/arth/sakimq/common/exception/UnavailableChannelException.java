package com.arth.sakimq.common.exception;

public class UnavailableChannelException extends MessageQueueException {

    public UnavailableChannelException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnavailableChannelException(String message) {
        super(message);
    }
}
