package com.example.url_shortner.exceptions;

public class InvalidPayloadException extends RuntimeException {
    public InvalidPayloadException(String message) {
        super(message);
    }
}
