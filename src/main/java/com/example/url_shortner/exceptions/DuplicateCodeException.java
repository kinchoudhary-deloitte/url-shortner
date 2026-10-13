package com.example.url_shortner.exceptions;


public class DuplicateCodeException extends RuntimeException {
    public DuplicateCodeException(String message) { super(message); }
}