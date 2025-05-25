package org.example.bitcask.net.exception;

public class UnknownStatusException extends Exception {
    private final int status;
    public UnknownStatusException(String message, int status) {
        super(message);
        this.status = status;
    }
    public int getStatus() {
        return status;
    }
}
