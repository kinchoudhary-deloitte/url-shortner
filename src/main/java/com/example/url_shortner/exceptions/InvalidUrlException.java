package com.example.url_shortner.exceptions;


public class InvalidUrlException extends RuntimeException {
    public InvalidUrlException(String message) { super(message); }
}
