package com.arth.sakimq.common.exception;

public class TimeoutException extends MessageQueueException {

    public TimeoutException(String message) {
        super(message);
    }

    public TimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
