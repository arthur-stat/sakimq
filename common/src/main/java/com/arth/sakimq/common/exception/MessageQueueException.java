package com.arth.sakimq.common.exception;

public abstract class MessageQueueException extends Exception {

    public MessageQueueException(String message) {
        super(message);
    }

    public MessageQueueException(String message, Throwable cause) {
        super(message, cause);
    }
}
