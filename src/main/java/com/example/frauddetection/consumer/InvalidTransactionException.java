package com.example.frauddetection.consumer;

/** A permanent business error: retrying cannot help, so the record goes straight to the DLT. */
public class InvalidTransactionException extends RuntimeException {

    public InvalidTransactionException(String message) {
        super(message);
    }
}
